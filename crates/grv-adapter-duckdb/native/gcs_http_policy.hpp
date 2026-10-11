#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/common/exception.hpp"

namespace grv {

// Parameter-only policy for a dedicated GCS engine. Install AFTER verified stock
// HTTPFS LOAD and BEFORE opening any remote handles:
//   auto &stock = config.GetHTTPUtil(); // capture before SetHTTPUtil
//   config.SetHTTPUtil(duckdb::make_shared_ptr<GCSHTTPPolicy>(stock));
// DBConfig retains replaced utilities in old_http_utils, keeping stock alive.
// Do not install on a general-purpose engine, replace HTTPFS afterwards, or reuse
// handles opened before installation. This is not a URL/access-control fence.
class GCSHTTPPolicy final : public duckdb::HTTPUtil {
public:
    explicit GCSHTTPPolicy(duckdb::HTTPUtil &stock, duckdb::string project = {}) : stock_(stock), project_(std::move(project)) {
    }

    duckdb::string GetName() const override {
        return stock_.GetName();
    }

    // Cover every virtual entry point: none may fall back to base HTTPParams.
    duckdb::unique_ptr<duckdb::HTTPParams>
    InitializeParameters(duckdb::optional_ptr<duckdb::FileOpener> opener,
                         duckdb::optional_ptr<duckdb::FileOpenerInfo> info) override {
        return Apply(stock_.InitializeParameters(opener, info));
    }

    duckdb::unique_ptr<duckdb::HTTPParams>
    InitializeParameters(duckdb::DatabaseInstance &db, const duckdb::string &path) override {
        return Apply(stock_.InitializeParameters(db, path));
    }

    duckdb::unique_ptr<duckdb::HTTPParams>
    InitializeParameters(duckdb::ClientContext &context, const duckdb::string &path) override {
        return Apply(stock_.InitializeParameters(context, path));
    }

    // Preserve stock transport even for callers using the wrapper directly.
    duckdb::unique_ptr<duckdb::HTTPClient>
    InitializeClient(duckdb::HTTPParams &params, const duckdb::string &host) override {
        return stock_.InitializeClient(params, host);
    }
    void CloseClient(duckdb::unique_ptr<duckdb::HTTPClient> &&client) override {
        stock_.CloseClient(std::move(client));
    }
    duckdb::unique_ptr<duckdb::HTTPResponse>
    SendRequest(duckdb::BaseRequest &request, duckdb::unique_ptr<duckdb::HTTPClient> &client) override {
        return stock_.SendRequest(request, client);
    }
    void LogRequest(duckdb::BaseRequest &request, duckdb::optional_ptr<duckdb::HTTPResponse> response) override {
        stock_.LogRequest(request, response);
    }
    bool ShouldRetry(const duckdb::BaseRequest &request, const duckdb::HTTPResponse &response) override {
        return stock_.ShouldRetry(request, response);
    }

    static constexpr uint64_t MAX_TIMEOUT_SECONDS = 30;

private:
    duckdb::unique_ptr<duckdb::HTTPParams> Apply(duckdb::unique_ptr<duckdb::HTTPParams> params) {
        if (!params || &params->http_util != &stock_) {
            throw duckdb::InternalException("GCS HTTP policy requires stock-owned parameters");
        }
        // Modify the original object, never slice HTTPFSParams or rebind its util.
        params->follow_location = false;
        params->override_verify_ssl = true;
        params->verify_ssl = true;
        params->http_proxy.clear();
        params->http_proxy_port = 0;
        params->http_proxy_username.clear();
        params->http_proxy_password.clear();
        params->logger.reset();
        params->retries = 0;
        if (!project_.empty()) { params->extra_headers["x-goog-user-project"] = project_; }
        // Zero means unbounded in stock clients. Keep shorter finite timeouts.
        if ((params->timeout == 0 && params->timeout_usec == 0) ||
            params->timeout >= MAX_TIMEOUT_SECONDS) {
            params->timeout = MAX_TIMEOUT_SECONDS;
            params->timeout_usec = 0;
        }
        return params;
    }

    duckdb::HTTPUtil &stock_;
    duckdb::string project_;
};

} // namespace grv
