// Offline native CAPI engine, static Parquet + signed stock HTTPFS.
#include "duckdb.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "httpfs.hpp"
#include "../../../crates/grv-adapter-duckdb/native/gcs_http_policy.hpp"
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <typeinfo>

extern "C" int grv_native_load_static_extensions(duckdb_database, char *, size_t);
using namespace duckdb;
static string Q(const string &s) {
    string out = "'";
    for (char c : s) { out += c; if (c == '\'') out += c; }
    return out + "'";
}
static void Require(bool ok) { if (!ok) throw std::runtime_error("probe assertion failed"); }
int main(int argc, char **argv) {
    duckdb_database db = nullptr;
    const char *stage="setup";
    try {
        Require(argc == 5 && string(DuckDB::LibraryVersion()) == "v1.5.6");
        string url = argv[2], fixture = argv[3], mode = argv[4];
        Require(url.rfind("http://127.0.0.1:", 0) == 0);
        duckdb_config cfg;
        Require(duckdb_create_config(&cfg) == DuckDBSuccess);
        for (auto k : {"autoinstall_known_extensions", "autoload_known_extensions", "allow_unsigned_extensions"})
            Require(duckdb_set_config(cfg, k, "false") == DuckDBSuccess);
        char *error = nullptr;
        auto status = duckdb_open_ext(nullptr, &db, cfg, &error);
        duckdb_destroy_config(&cfg); duckdb_free(error);
        Require(status == DuckDBSuccess);
        char static_error[2048] = {};
        Require(grv_native_load_static_extensions(db, static_error, sizeof(static_error)) == 0);
        {
            auto &database = *reinterpret_cast<DatabaseWrapper *>(db)->database;
            Connection c(database);
            auto exec = [&](const string &sql) {
                auto r = c.Query(sql);
                Require(!r->HasError());
                return r;
            };
            SecretManager::Get(*c.context).SetEnablePersistentSecrets(false);
            exec("LOAD " + Q(argv[1]));
            exec("SET enable_external_file_cache=false");
            // Deliberately hostile settings: policy must override after stock reads them.
            exec("SET http_timeout=999"); exec("SET http_retries=7");
            exec("SET enable_server_cert_verification=false");
            exec("SET http_proxy='http://127.0.0.1:1'");
            exec("SET http_proxy_username='synthetic-user'");
            exec("SET http_proxy_password='synthetic-password'");
            const char *token = std::getenv("GRV_POLICY_SYNTHETIC_TOKEN");
            Require(token && *token);
            exec("CREATE TEMPORARY SECRET policy_probe (TYPE http, PROVIDER config, BEARER_TOKEN " +
                 Q(token) + ", VERIFY_SSL false, SCOPE " + Q(url) + ")");
            exec("BEGIN TRANSACTION");
            stage="parameters";
            auto &config = DBConfig::GetConfig(*c.context);
            auto &stock = config.GetHTTPUtil(); // MUST precede SetHTTPUtil.
            ClientContextFileOpener opener(*c.context);
            FileOpenerInfo info = {url};
            auto before = stock.InitializeParameters(&opener, &info);
            stage="subtype";Require(dynamic_cast<HTTPFSParams *>(before.get()) != nullptr);
            stage="defaults";Require(before->follow_location && before->retries == 7 && before->timeout == 999);
            stage="hostile";Require(!before->verify_ssl && !before->http_proxy.empty());
            if (mode == "stock") {
                exec("SET http_proxy=''"); exec("SET http_retries=0");
            } else {
                config.SetHTTPUtil(make_shared_ptr<grv::GCSHTTPPolicy>(stock));
                auto check = [&](unique_ptr<HTTPParams> p) {
                    auto *p_ptr=p.get(), *before_ptr=before.get();
                    Require(typeid(*p_ptr) == typeid(*before_ptr) && &p->http_util == &stock);
                    Require(!p->follow_location && p->override_verify_ssl && p->verify_ssl);
                    Require(p->http_proxy.empty() && p->http_proxy_port == 0 &&
                            p->http_proxy_username.empty() && p->http_proxy_password.empty());
                    Require(!p->logger && p->retries == 0 && p->timeout == 30 && p->timeout_usec == 0);
                };
                auto &policy = config.GetHTTPUtil();
                stage="opener";check(policy.InitializeParameters(&opener, &info));
                stage="context";check(policy.InitializeParameters(*c.context, url));
                stage="database";check(policy.InitializeParameters(*database.instance, url));
                std::cout << "params: HTTPFS subtype + stock util retained; all 3 overloads hardened\n";
            }
            exec("COMMIT");
            stage="query";
            if (mode == "generate") {
                exec("COPY (SELECT 42::BIGINT AS id) TO " + Q(fixture) + " (FORMAT PARQUET)");
            } else {
                auto r = c.Query("SELECT id FROM read_parquet(" + Q(url) + ")");
                if (mode == "redirect") Require(r->HasError());
                else Require(!r->HasError() && r->RowCount() == 1 && r->GetValue(0, 0).GetValue<int64_t>() == 42);
                std::cout << "query: " << mode << " " << (r->HasError() ? "rejected" : "row=42") << "\n";
            }
            exec("DROP SECRET policy_probe");
        }
        duckdb_close(&db); return 0;
    } catch (...) {
        // Never print SQL, headers, token, or stock errors.
        duckdb_close(&db);
        std::cerr << "policy probe failed at " << stage << " (details suppressed)\n"; return 1;
    }
}
