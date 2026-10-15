// Package 32: GCS scanner rebind / cache / epoch / retained-handle /
// parallel-failure-isolation acceptance. Source-including proof only (includes
// shim.cpp); it is never compiled into the shipping library.
//
// The mock stock transport stands in for the stock delegate inside the ACTUAL
// shipping GCSHTTPPolicy::SendRequest (private parameter copy, boundary,
// client disposal). The GRVGuard bind sites, the Parquet scanner,
// ExactGCSFileSystem, the per-handle epoch gate and the GCSScanScope RAII are
// all the real shipping sources. Parameter construction is a model seeded
// by the real signed resolver outside queries; the socket leg is mocked. The live
// real-HTTPS case is planned separately (checkpoint case 12) and is NOT
// executed here. The supplementary GRVHTTPRequestGuard-registry path is not
// used here at all: the signed extension's private request path is known to
// bypass that registry, so every dispatch in this proof goes through the
// wrapper virtual SendRequest seam (or the wrapper-created client, which the
// mock also records, so no request path can escape the recorder).
#include "shim.cpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include <atomic>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <dirent.h>
#include <map>
#include <set>
#include <thread>
#include <sys/stat.h>
#include <functional>
#include <limits>
#include <cerrno>
namespace {
void Require(bool condition, const string &message) {
    if (!condition) {
        throw IOException("scanner lifecycle proof: " + message);
    }
}
// One owner's synthetic identity. The canary is a value, never a credential;
// it must never appear in any error, log, receipt or owned artifact.
struct OwnerCanary {
    string canary;
    string scope;
    string project;
};
// Qualification-only delegate: modeled parameter copies of real captured
// signed-resolution results, and an HTTP range/callback recorder. The ACTUAL
// shipping wrapper runs unchanged; this is not the real socket transport.
struct MockStock final : HTTPUtil {
    struct Record {
        string url;
        RequestType type;
        string bearer, project;
        uint64_t timeout, timeout_usec;
        bool follow_location;
        idx_t retries;
        bool via_wrapper;
    };
    struct TrackedClient final : HTTPClient {
        MockStock &mock;
        explicit TrackedClient(MockStock &mock) : mock(mock) {}
        ~TrackedClient() override { ++mock.clients_destroyed; }
        void Initialize(HTTPParams &) override {}
        unique_ptr<HTTPResponse> Get(GetRequestInfo &info) override { return mock.Handle(info, false); }
        unique_ptr<HTTPResponse> Head(HeadRequestInfo &info) override { return mock.Handle(info, false); }
        unique_ptr<HTTPResponse> Put(PutRequestInfo &) override { throw PermissionException("mock read-only"); }
        unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &) override { throw PermissionException("mock read-only"); }
        unique_ptr<HTTPResponse> Post(PostRequestInfo &) override { throw PermissionException("mock read-only"); }
    };
    int clients_created = 0, clients_destroyed = 0;
    vector<Record> records;
    map<string, string> content;
    set<string> fail_head, fail_get, reject_content;
    // Captured from the REAL installed signed parameter resolver, outside any
    // query. Initialization below is a qualification-only parameter factory,
    // not a claim that the mock reimplements signed secret lookup. Reentering
    // RunFunctionInTransaction during bind would deadlock the query-held lock.
    unique_ptr<HTTPParams> captured;
    string gate_url;
    bool gate_armed = false;
    std::atomic<bool> in_gate{false}, gate_released{false};
    ClientContext *gate_context = nullptr;
    size_t RecordsFor(const string &url, RequestType type) const {
        size_t n = 0;
        for (const auto &r : records) if (r.url == url && r.type == type) ++n;
        return n;
    }
    size_t TotalRecords() const { return records.size(); }
    unique_ptr<HTTPParams> Parameters() {
        Require(bool(captured), "missing captured signed parameters");
        auto &p = captured->Cast<HTTPFSParams>();
        auto n = make_uniq<HTTPFSParams>(*this);
        n->timeout=p.timeout; n->timeout_usec=p.timeout_usec; n->retries=p.retries;
        n->retry_wait_ms=p.retry_wait_ms; n->retry_backoff=p.retry_backoff; n->keep_alive=p.keep_alive;
        n->follow_location=p.follow_location; n->override_verify_ssl=p.override_verify_ssl; n->verify_ssl=p.verify_ssl;
        n->http_proxy=p.http_proxy; n->http_proxy_port=p.http_proxy_port;
        n->http_proxy_username=p.http_proxy_username; n->http_proxy_password=p.http_proxy_password;
        n->extra_headers=p.extra_headers; n->logger=p.logger;
        n->force_download=p.force_download; n->auto_fallback_to_full_download=p.auto_fallback_to_full_download;
        n->enable_server_cert_verification=p.enable_server_cert_verification;
        n->enable_curl_server_cert_verification=p.enable_curl_server_cert_verification;
        n->hf_max_per_page=p.hf_max_per_page; n->ca_cert_file=p.ca_cert_file; n->bearer_token=p.bearer_token;
        n->unsafe_disable_etag_checks=p.unsafe_disable_etag_checks; n->s3_version_id_pinning=p.s3_version_id_pinning;
        n->state=p.state; n->user_agent=p.user_agent; n->pre_merged_headers=p.pre_merged_headers;
        n->force_download_threshold=p.force_download_threshold;
        return std::move(n);
    }
    unique_ptr<HTTPParams> InitializeParameters(optional_ptr<FileOpener>, optional_ptr<FileOpenerInfo>) override {
        return Parameters();
    }
    unique_ptr<HTTPParams> InitializeParameters(DatabaseInstance &, const string &) override { return Parameters(); }
    unique_ptr<HTTPParams> InitializeParameters(ClientContext &, const string &) override { return Parameters(); }
    unique_ptr<HTTPClient> InitializeClient(HTTPParams &, const string &) override {
        ++clients_created;
        return make_uniq<TrackedClient>(*this);
    }
    unique_ptr<HTTPResponse> SendRequest(BaseRequest &request, unique_ptr<HTTPClient> &client) override {
        client = make_uniq<TrackedClient>(*this);
        ++clients_created;
        return Handle(request, true);
    }
    static size_t Decimal(const string &text) {
        Require(!text.empty(), "empty range number");
        size_t n = 0;
        for (auto c : text) {
            Require(c >= '0' && c <= '9', "invalid range number");
            size_t digit = size_t(c - '0');
            Require(n <= (std::numeric_limits<size_t>::max() - digit) / 10, "range overflow");
            n = n * 10 + digit;
        }
        return n;
    }
    unique_ptr<HTTPResponse> Handle(BaseRequest &request, bool via_wrapper) {
        auto &p = request.params.Cast<HTTPFSParams>();
        auto found = p.extra_headers.find("x-goog-user-project");
        records.push_back({request.url, request.type, p.bearer_token,
            found == p.extra_headers.end() ? string() : found->second,
            p.timeout, p.timeout_usec, p.follow_location, p.retries, via_wrapper});
        auto it = content.find(request.url);
        if (it == content.end()) return make_uniq<HTTPResponse>(static_cast<HTTPStatusCode>(404));
        if (request.type == RequestType::HEAD_REQUEST) {
            if (fail_head.count(request.url)) throw IOException("injected HEAD failure");
            auto response = make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
            response->headers.Insert("Content-Length", to_string(it->second.size()));
            return response;
        }
        Require(request.type == RequestType::GET_REQUEST, "unexpected request method");
        if (fail_get.count(request.url)) throw IOException("injected GET failure");
        auto &get = request.Cast<GetRequestInfo>();
        const auto &body = it->second;
        size_t start = 0, finish = body.size();
        bool ranged = request.headers.HasHeader("Range");
        if (ranged) {
            auto text = request.headers.GetHeaderValue("Range");
            Require(StringUtil::StartsWith(text, "bytes="), "unsupported range");
            auto dash = text.find('-', 6);
            Require(dash != string::npos, "missing range end");
            start = Decimal(text.substr(6, dash - 6));
            auto end = Decimal(text.substr(dash + 1));
            Require(end >= start && end != std::numeric_limits<size_t>::max(), "invalid range bounds");
            if (start >= body.size()) {
                auto response = make_uniq<HTTPResponse>(static_cast<HTTPStatusCode>(416));
                if (get.response_handler) get.response_handler(*response);
                return response;
            }
            finish = std::min(body.size(), end + 1);
        }
        auto response = make_uniq<HTTPResponse>(static_cast<HTTPStatusCode>(ranged ? 206 : 200));
        response->headers.Insert("Content-Length", to_string(finish - start));
        if (ranged) response->headers.Insert("Content-Range", "bytes " + to_string(start) + "-" +
            to_string(finish - 1) + "/" + to_string(body.size()));
        if (get.response_handler && !get.response_handler(*response)) {
            response->success = false; response->request_error = "synthetic response rejection";
            return response;
        }
        if (gate_armed && request.url == gate_url) {
            in_gate.store(true, std::memory_order_release);
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (!gate_released.load(std::memory_order_acquire) &&
                   !(gate_context && gate_context->IsInterrupted()) && std::chrono::steady_clock::now() < deadline)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            in_gate.store(false, std::memory_order_release);
        }
        auto deliver = [&](size_t off, size_t count) {
            return !reject_content.count(request.url) && (!get.content_handler ||
                get.content_handler(reinterpret_cast<const_data_ptr_t>(body.data() + off), count));
        };
        size_t half = (finish - start) / 2;
        bool ok = (half == 0 || deliver(start, half)) && deliver(start + half, finish - start - half);
        if (!ok) { response->success = false; response->request_error = "synthetic body rejection"; }
        return response;
    }
    void ReleaseGate() { gate_released.store(true, std::memory_order_release); }
};

// Bounded metadata-FFI result decoding: u64 rows, u64 columns, then per cell
// one validity byte, an eight-byte little-endian length and the text bytes.
struct Decoded {
    uint64_t rows = 0, cols = 0;
    vector<vector<string>> cells;
    vector<vector<bool>> valid;
};
Decoded Decode(const unsigned char *output, size_t written) {
    Require(written >= 16, "short metadata header");
    auto u64le = [&](size_t off) {
        Require(off <= written && written-off >= 8, "short metadata integer");
        uint64_t v=0;
        for(size_t i=0;i<8;++i) v |= uint64_t(output[off+i]) << (8*i);
        return v;
    };
    Decoded d; d.rows=u64le(0); d.cols=u64le(8);
    Require(d.cols<=16 && (d.rows==0 || d.cols>0), "invalid metadata dimensions");
    Require(d.rows==0 || d.rows <= (written-16)/(9*d.cols), "impossible metadata rows");
    size_t off=16;
    for(uint64_t r=0;r<d.rows;++r) {
        vector<string> row; vector<bool> valid;
        for(uint64_t c=0;c<d.cols;++c) {
            Require(off<written && output[off]<=1, "invalid cell validity");
            bool present=output[off++]!=0;
            uint64_t len=u64le(off); off+=8;
            Require(len<=written-off && (present || len==0), "invalid cell length");
            row.emplace_back(reinterpret_cast<const char *>(output+off), size_t(len));
            valid.push_back(present); off+=size_t(len);
        }
        d.cells.push_back(std::move(row)); d.valid.push_back(std::move(valid));
    }
    Require(off==written, "trailing metadata bytes");
    return d;
}
// Threads cannot escape a failing assertion. Cleanup releases a synthetic gate
// and requests supported interruption before joining. An external test timeout
// bounds a deadlocked TEST PROCESS; this does not prove a finite native join.
struct JoinedWorker {
    std::exception_ptr failure;
    std::function<void()> cleanup;
    std::thread worker;
    template<class F> JoinedWorker(F work, std::function<void()> cleanup = [](){})
        : cleanup(std::move(cleanup)), worker([this, work](){try { work(); } catch(...) {failure=std::current_exception();}}) {}
    ~JoinedWorker(){if(worker.joinable()){cleanup();worker.join();}}
    void Join(){if(worker.joinable())worker.join();}
    void Rethrow(){if(failure)std::rethrow_exception(failure);}
};
struct FD {
    int value;
    explicit FD(int value):value(value){Require(value>=0,"fixture descriptor open");}
    ~FD(){::close(value);}
};
struct InterruptRef {
    void *value;
    explicit InterruptRef(void *owner):value(grv_native_interrupt_handle(owner)){Require(value!=nullptr,"interrupt handle");}
    ~InterruptRef(){grv_native_interrupt_close(value);}
};
template<class F> bool Wait(F ready, std::chrono::seconds duration) {
    auto deadline=std::chrono::steady_clock::now()+duration;
    while(!ready() && std::chrono::steady_clock::now()<deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    return ready() && std::chrono::steady_clock::now()<=deadline;
}
void CanaryFreeArtifacts(const string &root, const vector<string> &canaries) {
    struct stat root_stat;
    Require(::lstat(root.c_str(), &root_stat)==0 && S_ISDIR(root_stat.st_mode), "owned root inventory");
    auto *raw=::opendir(root.c_str());
    Require(raw!=nullptr,"owned root unreadable");
    std::unique_ptr<DIR, int(*)(DIR*)> directory(raw,::closedir);
    size_t files=0;
    for(;;) {
        errno=0; auto *entry=::readdir(directory.get());
        if(!entry){Require(errno==0,"owned inventory incomplete");break;}
        if(!strcmp(entry->d_name,".") || !strcmp(entry->d_name,".."))continue;
        auto path=root+"/"+entry->d_name;
        struct stat st;
        Require(::lstat(path.c_str(),&st)==0 && S_ISREG(st.st_mode),"unexpected/missing owned regular artifact");
        std::ifstream file(path,std::ios::binary);
        Require(bool(file),"owned artifact unreadable");
        size_t keep=0; for(const auto &c:canaries)keep=std::max(keep,c.size());
        string tail; char bytes[65536]; size_t total=0;
        while(file.read(bytes,sizeof(bytes)) || file.gcount()) {
            auto n=size_t(file.gcount()); total+=n;
            string joined=tail+string(bytes,n);
            for(const auto &c:canaries)Require(joined.find(c)==string::npos,"canary persisted in owned artifact");
            tail=joined.substr(joined.size()>keep?joined.size()-keep:0);
        }
        Require(file.eof() && !file.bad() && total==size_t(st.st_size),"owned artifact incomplete read");
        ++files;
    }
    Require(files>0,"empty artifact scan");
}
void DecoderRefusals() {
    unsigned char good[25]={}; good[0]=1; good[8]=1;
    auto ok=Decode(good,sizeof(good));
    Require(ok.rows==1 && ok.cols==1 && !ok.valid[0][0],"decoder positive null cell");
    for(size_t length: {size_t(0),size_t(15),size_t(16),size_t(17),size_t(24)}) {
        bool refused=false;try { Decode(good,length); } catch(const IOException &){refused=true;}
        Require(refused,"short packet decoder accepted");
    }
    unsigned char bad[26]; memcpy(bad,good,sizeof(good));bad[25]=0;
    bool refused=false;try {Decode(bad,sizeof(bad));}catch(const IOException &){refused=true;}
    Require(refused,"trailing byte decoder accepted");
    memcpy(bad,good,sizeof(good));bad[8]=17;
    refused=false;try {Decode(bad,25);}catch(const IOException &){refused=true;}
    Require(refused,"oversized dimension decoder accepted");
    memcpy(bad,good,sizeof(good));bad[16]=1;memset(bad+17,255,8);
    refused=false;try {Decode(bad,25);}catch(const IOException &){refused=true;}
    Require(refused,"overflow length decoder accepted");
}

void RemoveTree(const string &path) {
    struct dirent *entry = nullptr;
    auto dir = ::opendir(path.c_str());
    if (dir) {
        while ((entry = ::readdir(dir)) != nullptr) {
            if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) {
                continue;
            }
            string child = path + "/" + entry->d_name;
            struct stat st;
            if (::lstat(child.c_str(), &st) == 0) {
                if (S_ISDIR(st.st_mode)) {
                    RemoveTree(child);
                } else {
                    ::unlink(child.c_str());
                }
            }
        }
        ::closedir(dir);
    }
    ::rmdir(path.c_str());
}
// Generate a one-row parquet fixture with the pinned static extension in a
// plain (unguarded) engine; returns the file bytes.
string MakeParquetBytes(const string &path, const string &column, const string &value) {
    {
        DuckDB db(nullptr);
        db.LoadStaticExtension<ParquetExtension>();
        Connection c(db);
        auto type = (column == "val_str") ? "VARCHAR" : "BIGINT";
        auto name = (column == "val_str") ? "val" : column;
        auto quoted = "'" + StringUtil::Replace(value, "'", "''") + "'";
        auto result = c.Query("COPY (SELECT " + quoted + "::" + type + " AS " + name + ") TO '" + path +
                               "' (FORMAT PARQUET)");
        if (result->HasError()) {
            throw IOException("fixture generation failed: " + result->GetError());
        }
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw IOException("fixture unreadable: " + path);
    }
    return string((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
}
// Swap the configured owner's stock delegate for the mock. The shipping
// wrapper code (GCSHTTPPolicy) is unchanged; only the stock it delegates to
// is the recorder.
shared_ptr<grv::GCSHTTPPolicy> InstallMock(Engine &engine, MockStock &mock, const string &project) {
    auto &config = DBConfig::GetConfig(*engine.connection.context);
    auto state = engine.exact_gcs;
    auto wrapper = make_shared_ptr<grv::GCSHTTPPolicy>(
        mock, project, [state](BaseRequest &request, unique_ptr<HTTPClient> &client) {
            state->BeforeRequest(request, client);
        });
    config.SetHTTPUtil(wrapper);
    return wrapper;
}
struct ProofState {
    string root;
    OwnerCanary a, b, c, d, e, f;
    // Owner A URLs (scope a)
    string u1, u2, u3, u4, u5, uX;
    // Other owners
    string uD1, uC1;
    string localParquet;
    MockStock mockA, mockB, mockC, mockD, mockE, mockF;
    void *ownerA = nullptr, *ownerB = nullptr, *ownerC = nullptr, *ownerD = nullptr, *ownerE = nullptr, *ownerF = nullptr;
    void CloseOwners() {
        for (auto &owner : {&ownerA, &ownerB, &ownerC, &ownerD, &ownerE, &ownerF}) {
            if (*owner) {
                grv_native_close(*owner);
                *owner = nullptr;
            }
        }
    }
    ~ProofState() { CloseOwners(); RemoveTree(root); }
};
void ConfigureOwner(ProofState *s, void *&owner, const char *extensions, const OwnerCanary &canary,
                    MockStock &mock) {
    char error[4096] = {0};
    owner = grv_native_open((s->root + "/" + canary.project + ".duckdb").c_str(), error, sizeof(error));
    if (!owner) {
        throw IOException("owner open failed: " + string(error));
    }
    auto &engine = *static_cast<Owner *>(owner)->engine;
    if (grv_native_gcs_configure(owner, extensions, canary.scope.c_str(), canary.canary.c_str(),
                                  canary.project.c_str(), 300000, error, sizeof(error)) != 0) {
        throw IOException("configure " + canary.project + " failed: " + string(error));
    }
    if (string(error).find(canary.canary) != string::npos) {
        throw IOException("configure error carried a canary");
    }
    auto &original = DBConfig::GetConfig(*engine.connection.context).GetHTTPUtil();
    engine.connection.context->RunFunctionInTransaction([&](){
        mock.captured=original.InitializeParameters(*engine.connection.context, canary.scope+"/captured.parquet");
    });
    Require(mock.captured && mock.captured->Cast<HTTPFSParams>().bearer_token==canary.canary, "actual signed resolver canary mismatch");
    InstallMock(engine, mock, canary.project);
}
// Drains all rows of a small single-column oracle (never a first-row-only
// assertion masquerading as a multi-row check).
struct FirstRow {
    vector<Value> values;
    idx_t rows = 0;
};
template <class T>
FirstRow ReadFirstRow(T &&result) {
    FirstRow out;
    while (auto chunk = result->Fetch()) {
        Require(chunk->ColumnCount()==1, "expected single-column oracle");
        for (idx_t r=0; r<chunk->size(); ++r) out.values.push_back(chunk->GetValue(0, r));
        out.rows += chunk->size();
    }
    return out;
}
}
int main(int argc, char **argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    if (argc != 2) {
        return 2;
    }
    const char *extensionsDir = argv[1];
    // configure LOADs the signed extension file, not the directory.
    string httpfs = string(extensionsDir) + "/httpfs.duckdb_extension";
    const char *extensions = httpfs.c_str();
    ProofState s;
    try {
        char pattern[] = "/tmp/grv-gcs-scanner-lifecycle-XXXXXX";
        auto base = ::mkdtemp(pattern);
        if (!base) {
            return 2;
        }
        s.root = base;
        std::printf("OWNED_LOCAL_FIXTURE %s\n", base);
        DecoderRefusals();
        std::printf("PASS decoder malformed/overflow/exact-consumption controls\n");
        // --- identities and fixtures -------------------------------------------------
        s.a = {"synthetic-canary-owner-a-not-a-credential", "https://storage.googleapis.com/owner-a-bucket/data",
                "project-a"};
        s.b = {"synthetic-canary-owner-b-not-a-credential", "https://storage.googleapis.com/owner-b-bucket/data",
                "project-b"};
        s.c = {"synthetic-canary-owner-c-not-a-credential", "https://storage.googleapis.com/owner-c-bucket/data",
                "project-c"};
        s.d = {"synthetic-canary-owner-d-not-a-credential", "https://storage.googleapis.com/owner-d-bucket/data",
                "project-d"};
        s.e = {s.a.canary, s.a.scope, "project-e"}; // identical synthetic token value to A
        s.f = {"synthetic-canary-owner-f-not-a-credential", "https://storage.googleapis.com/owner-f-bucket/data", "project-f"};
        s.u1 = s.a.scope + "/object-a.parquet";
        s.u2 = s.a.scope + "/object-b.parquet";
        s.u3 = s.a.scope + "/object-c.parquet";
        s.u4 = s.a.scope + "/object-d.parquet";
        s.u5 = s.a.scope + "/object-e.parquet";
        s.uX = s.a.scope + "/object-x.parquet";
        s.uD1 = s.d.scope + "/object-d1.parquet";
        s.uC1 = s.c.scope + "/object-c1.parquet";
        s.localParquet = s.root + "/local.parquet";
        auto bytes111 = MakeParquetBytes(s.root + "/f111.parquet", "id", "111");
        auto bytes222 = MakeParquetBytes(s.root + "/f222.parquet", "id", "222");
        auto bytes333 = MakeParquetBytes(s.root + "/f333.parquet", "id", "333");
        auto bytes444 = MakeParquetBytes(s.root + "/f444.parquet", "val", "444");
        auto bytesX = MakeParquetBytes(s.root + "/fx.parquet", "val_str", "x");
        auto bytes777 = MakeParquetBytes(s.root + "/f777.parquet", "id", "777");
        auto bytes888 = MakeParquetBytes(s.root + "/f888.parquet", "id", "888");
        auto bytes37 = MakeParquetBytes(s.localParquet, "value", "37");
        s.mockA.content[s.u1] = bytes111;
        s.mockA.content[s.u2] = bytes222;
        s.mockA.content[s.u3] = bytes333;
        s.mockA.content[s.u4] = bytes444;
        s.mockA.content[s.u5] = bytesX;
        s.mockE.content[s.u1] = bytes111;
        s.mockD.content[s.uD1] = bytes777;
        s.mockC.content[s.uC1] = bytes888;
        // --- owner A: the rebind/epoch/params/mixed-local matrix ---------------------
        ConfigureOwner(&s, s.ownerA, extensions, s.a, s.mockA);
        auto &engineA = *static_cast<Owner *>(s.ownerA)->engine;
        // The guard-approved views are the supported rebind targets (the same
        // relations mechanism the guard uses for its own views).
        auto &policyA = *GRVGuard::Get(*engineA.connection.context);
        policyA.relations.insert("main.gcs_probe");
        policyA.relations.insert("main.gcs_wrap");
        auto query = [&](Engine &engine, const string &sql) {
            std::printf("STEP query %.24s\n", sql.c_str());
            auto result = engine.connection.Query(sql);
            if (result->HasError()) {
                throw IOException("query failed: " + result->GetError());
            }
            return result;
        };
        auto refused = [&](Engine &engine, const string &sql) {
            auto result = engine.connection.Query(sql);
            Require(result->HasError(), "forbidden query was allowed: " + sql);
            string error = result->GetError();
            Require(error.find(s.a.canary) == string::npos && error.find(s.b.canary) == string::npos,
                    "refusal error carried a canary: " + sql);
            return error;
        };
        auto scan = [&](const string &url) {
            return "SELECT id FROM read_parquet('" + url + "', hive_partitioning=false, union_by_name=false)";
        };
        // A handle opened in scope1 is retained across the scope boundary; it
        // is the canonical stale-handle object for the epoch assertions. The
        // prepared statement bound in scope2 is likewise retained: executing it
        // in a later scope exercises the stale-binding route.
        auto directOpen = [&](const string &url, FileOpenFlags flags) {
            unique_ptr<FileHandle> file;
            engineA.connection.context->RunFunctionInTransaction([&](){
                file=FileSystem::GetFileSystem(*engineA.connection.context).OpenFile(url, flags);
            });
            return file;
        };
        unique_ptr<FileHandle> retained;
        unique_ptr<PreparedStatement> retainedPrepared;
        std::printf("START cases1-2 actual scanner/rebind/epoch\n");
        // --- case 1: genuine automatic rebind with a positive physical oracle --------
        {
            GCSScanScope scope1(engineA, {s.u1});
            query(engineA, "CREATE VIEW gcs_probe AS " + scan(s.u1));
            query(engineA, "CREATE VIEW gcs_wrap AS SELECT * FROM gcs_probe");
            std::printf("STEP initial prepare\n");
            auto prepared = engineA.connection.Prepare("SELECT id FROM gcs_probe");
            Require(!prepared->HasError(), "allowed initial prepare failed");
            std::printf("STEP initial execute\n");
            auto first = prepared->Execute();
            Require(!first->HasError(), "initial scan failed: " + first->GetError());
            auto firstRow = ReadFirstRow(first);
            Require(firstRow.rows == 1 && firstRow.values[0].GetValue<int64_t>() == 111,
                    "initial physical scan returned the wrong value");
            // The nested wrapper view scans through the same admitted target.
            auto wrapped = query(engineA, "SELECT id FROM gcs_wrap");
            auto wrappedRow = ReadFirstRow(wrapped);
            Require(!wrappedRow.values.empty() && wrappedRow.values[0].GetValue<int64_t>() == 111,
                    "wrapper view scan wrong");
            // The mock witnessed the real dispatch with A's own canary.
            Require(s.mockA.RecordsFor(s.u1, RequestType::GET_REQUEST) >= 1, "no GET was dispatched for u1");
            auto &last = s.mockA.records.back();
            Require(last.bearer == s.a.canary, "dispatched request did not carry owner A's canary");
            Require(last.project == "project-a", "dispatched request lost project routing");
            // Open a direct exact-filesystem handle in this scope (epoch 1)
            // and retain it across the scope boundary.
            auto &vfs = FileSystem::GetFileSystem(*engineA.connection.context);
            std::printf("STEP direct handle open\n");
            retained = directOpen(s.u1, FileOpenFlags::FILE_FLAGS_READ);
            Require(retained != nullptr, "admitted handle open failed");
            char buffer[4096];
            retained->file_system.Seek(*retained, 0);
            int64_t n = retained->file_system.Read(*retained, buffer, sizeof(buffer));
            Require(n > 0 && memcmp(buffer, "PAR1", 4) == 0,
                     "live same-epoch handle read did not return parquet bytes");
            // Writable opens are refused at the exact filesystem.
            bool writeRefused = false;
            try {
                directOpen(s.u1, FileOpenFlags::FILE_FLAGS_WRITE);
            } catch (const PermissionException &) {
                writeRefused = true;
            }
            Require(writeRefused, "writable open flag was accepted on a GCS URL");
            // Glob never widens: the exact admitted path returns only itself.
            auto infos = vfs.Glob(s.u1);
            Require(infos.size() == 1 && infos[0].path == s.u1, "glob widened the admitted path");
            bool globRefused = false;
            try {
                vfs.Glob(s.u1 + "/*");
            } catch (const PermissionException &) {
                globRefused = true;
            }
            Require(globRefused, "glob pattern on a non-admitted path was admitted");
        }
        // --- case 2a: stale handle after ScopeEnd; SAME URI re-admitted ---------------
        {
            // The scope has ended: the retained epoch-1 handle is refused
            // before any stock read even though u1 will be re-admitted below.
            char buffer[8];
            bool staleRefused = false;
            try {
                retained->file_system.Seek(*retained, 0);
                retained->file_system.Read(*retained, buffer, 4);
            } catch (const PermissionException &) {
                staleRefused = true;
            }
            Require(staleRefused, "stale handle was accepted after the scope ended");
            size_t before = s.mockA.TotalRecords();
            GCSScanScope scope2(engineA, {s.u1, s.u2});
            // Re-admitting the SAME uri in a new scope does not revive the
            // old handle: the epoch gate is per-handle.
            staleRefused = false;
            try {
                retained->file_system.Seek(*retained, 0);
                retained->file_system.Read(*retained, buffer, 4);
            } catch (const PermissionException &) {
                staleRefused = true;
            }
            Require(staleRefused, "re-admitted same URI revived a stale-epoch handle");
            Require(s.mockA.TotalRecords() == before, "stale handle reached the stock transport");
            // A fresh handle in the new scope is accepted (live exact handle,
            // same epoch).
            auto &vfs = FileSystem::GetFileSystem(*engineA.connection.context);
            auto fresh = directOpen(s.u1, FileOpenFlags::FILE_FLAGS_READ);
            Require(fresh != nullptr, "fresh handle in the new scope failed");
            char buffer2[8];
            fresh->file_system.Seek(*fresh, 0);
            Require(fresh->file_system.Read(*fresh, buffer2, 4) == 4 && memcmp(buffer2, "PAR1", 4) == 0,
                    "fresh same-epoch handle read failed");
            // --- case 1b: the genuine automatic rebind to a new admitted URL ---------
            retainedPrepared = engineA.connection.Prepare("SELECT id FROM gcs_probe");
            Require(!retainedPrepared->HasError(), "prepare before rebind failed");
            query(engineA, "CREATE OR REPLACE VIEW gcs_probe AS " + scan(s.u2));
            auto rebound = retainedPrepared->Execute();
            Require(!rebound->HasError(), "automatic rebind failed: " + rebound->GetError());
            auto reboundRow = ReadFirstRow(rebound);
            Require(!reboundRow.values.empty() && reboundRow.values[0].GetValue<int64_t>() == 222,
                    "automatic rebind did not scan the new admitted URL");
            Require(s.mockA.RecordsFor(s.u2, RequestType::GET_REQUEST) >= 1, "rebind scan never dispatched u2");
            // --- case 2b: view replacement to an unadmitted URL is refused ----------
            size_t beforeX = s.mockA.TotalRecords();
            bool createRefused = false;
            string createError;
            try {
                query(engineA, "CREATE OR REPLACE VIEW gcs_probe AS " + scan(s.uX));
            } catch (const std::exception &failure) {
                createRefused = true;
                createError = failure.what();
            }
            Require(createRefused, "view replacement to an unadmitted URL was admitted");
            Require(createError.find(s.a.canary) == string::npos, "unadmitted-URL refusal carried a canary");
            Require(s.mockA.TotalRecords() == beforeX, "unadmitted URL reached the transport");
            Require(s.mockA.RecordsFor(s.uX, RequestType::GET_REQUEST) == 0 &&
                        s.mockA.RecordsFor(s.uX, RequestType::HEAD_REQUEST) == 0,
                    "unadmitted URL produced a transport record");
            // The view is unchanged: the previous binding still serves u2.
            auto still = engineA.connection.Prepare("SELECT id FROM gcs_probe");
            auto stillResult = still->Execute();
            auto stillRow = ReadFirstRow(stillResult);
            Require(!stillResult->HasError() && !stillRow.values.empty() && stillRow.values[0].GetValue<int64_t>() == 222,
                    "refused replacement corrupted the live view binding");
        }
        // Executed bind data may legitimately open a fresh handle next epoch.
        {
            GCSScanScope scope3(engineA, {s.u2});
            auto before=s.mockA.TotalRecords();
            auto result=retainedPrepared->Execute();
            auto row=ReadFirstRow(result);
            Require(!result->HasError() && row.rows==1 && row.values[0].GetValue<int64_t>()==222,
                    "retained prepared execution with fresh admitted handle failed");
            Require(s.mockA.TotalRecords()>before, "fresh execution route not observed");
        }
        {
            GCSScanScope unadmitted(engineA, {s.u1});
            auto before=s.mockA.TotalRecords();
            auto result=retainedPrepared->Execute();
            Require(result->HasError(), "retained prepared execution escaped current admission");
            Require(s.mockA.TotalRecords()==before, "unadmitted prepared route dispatched");
        }
        std::printf("PASS cases1-2 scanner/rebind, stale handles, fresh and denied prepared routes\n");
        // --- case 3: fixed explicit cast / typed null / positional union semantics ----
        {
            GCSScanScope scope4(engineA, {s.u1, s.u4, s.u5});
            auto cast = query(engineA, "SELECT id::BIGINT AS x FROM read_parquet('" + s.u1 +
                                                    "', hive_partitioning=false, union_by_name=false)");
            auto castRow = ReadFirstRow(cast);
            Require(!castRow.values.empty() && castRow.values[0].GetValue<int64_t>() == 111, "explicit cast scan wrong");
            auto nulls = query(engineA, "SELECT NULL::BIGINT AS x FROM read_parquet('" + s.u1 +
                                                      "', hive_partitioning=false, union_by_name=false)");
            auto nullsRow = ReadFirstRow(nulls);
            Require(nullsRow.rows == 1 && nullsRow.values[0].IsNull(), "typed null projection wrong");
            // Positional union with DIFFERENT column names: union_by_name=false
            // stays in force (a name-union would refuse id vs val).
            auto positional = query(engineA, "SELECT * FROM read_parquet('" + s.u1 +
                                                        "', hive_partitioning=false, union_by_name=false) UNION ALL "
                                                        "SELECT * FROM read_parquet('" +
                                      s.u4 + "', hive_partitioning=false, union_by_name=false)");
            auto positionalRow = ReadFirstRow(positional);
            Require(positionalRow.rows == 2, "positional union row count wrong");
            Require(positionalRow.values[0].GetValue<int64_t>() == 111 &&
                        positionalRow.values[1].GetValue<int64_t>() == 444,
                    "positional union values wrong");
            // GRV generated views cast explicitly. Ordinary SQL unions may
            // coerce; this fixture instead proves invalid explicit conversion
            // cannot silently widen the fixed BIGINT output.
            refused(engineA, "SELECT CAST(val AS BIGINT) FROM read_parquet('" + s.u5 +
                    "', hive_partitioning=false, union_by_name=false)");
            std::printf("PASS case3 fixed cast/typed-null/union_by_name semantics stay exact\n");
        }
        // --- case 5: copied bind + cold/warm re-dispatch under shipping settings -----
        {
            GCSScanScope scope5(engineA, {s.u1});
            auto prepared = engineA.connection.Prepare(scan(s.u1));
            Require(!prepared->HasError(), "cold prepare failed");
            // A distinct statement object sharing the same bound data: the
            // clone/copy-bind route (no rebind, same epoch, same handle).
            auto copied = make_uniq<PreparedStatement>(prepared->context, prepared->data, prepared->query,
                                                        prepared->named_param_map);
            auto cold = prepared->Execute();
            auto coldRow = ReadFirstRow(cold);
            Require(!cold->HasError() && !coldRow.values.empty() && coldRow.values[0].GetValue<int64_t>() == 111, "cold scan wrong");
            size_t coldGets = s.mockA.RecordsFor(s.u1, RequestType::GET_REQUEST);
            size_t coldHeads = s.mockA.RecordsFor(s.u1, RequestType::HEAD_REQUEST);
            Require(coldGets >= 1, "cold scan did not dispatch a GET");
            auto warm = copied->Execute();
            auto warmRow = ReadFirstRow(warm);
            Require(!warm->HasError() && !warmRow.values.empty() && warmRow.values[0].GetValue<int64_t>() == 111,
                    "copied-bind warm scan wrong");
            // HTTPFS handle-adaptive buffering can remain even when engine
            // caches are disabled. Observe counts rather than invent a
            // guaranteed warm request pattern; epoch denial is tested above.
            std::printf("OBSERVED warm deltas HEAD=%zu GET=%zu\n",
                s.mockA.RecordsFor(s.u1,RequestType::HEAD_REQUEST)-coldHeads,
                s.mockA.RecordsFor(s.u1,RequestType::GET_REQUEST)-coldGets);
            std::printf("PASS case5 copied bind + cold/warm re-dispatch observed on the real reader/cache path\n");
        }
        // --- case 6: typed parameters through all three entry points ---------------
        {
            auto &config = DBConfig::GetConfig(*engineA.connection.context);
            auto &wrapper = config.GetHTTPUtil();
            GCSScanScope scope6(engineA, {s.u1});
            auto runEntry = [&](auto makeParams, const char *label) {
                unique_ptr<HTTPParams> params = makeParams();
                Require(params != nullptr, string(label) + " produced no parameters");
                auto &fs = params->Cast<HTTPFSParams>();
                Require(fs.bearer_token == s.a.canary, string(label) + " did not resolve owner A's bearer");
                auto project = fs.extra_headers.find("x-goog-user-project");
                Require(project != fs.extra_headers.end() && project->second == "project-a",
                        string(label) + " lost project routing");
                // The dispatch must use a private copy: mutate the original to
                // a sentinel and verify the recorded request is bounded while
                // the original keeps the sentinel.
                fs.timeout = 999;
                fs.timeout_usec = 0;
                HTTPHeaders headers;
                HeadRequestInfo request(s.u1, headers, *params);
                unique_ptr<HTTPClient> client;
                auto response = wrapper.SendRequest(request, client);
                Require(response && response->status == HTTPStatusCode::OK_200,
                        string(label) + " dispatch was refused");
                Require(!client, "request client survived the wrapper dispatch");
                auto &recorded = s.mockA.records.back();
                Require(recorded.timeout <= 30, string(label) + " dispatched an unbounded timeout");
                Require(recorded.bearer == s.a.canary, string(label) + " dispatched the wrong bearer");
                Require(fs.timeout == 999, string(label) + " mutated the original shared parameters");
                Require(fs.bearer_token == s.a.canary, string(label) + " zeroed the original bearer");
            };
            runEntry([&]() {
                ClientContextFileOpener opener(*engineA.connection.context);
                FileOpenerInfo info;
                info.file_path = s.u1;
                return wrapper.InitializeParameters(opener, &info);
            }, "opener+info entry point");
            runEntry([&]() { return wrapper.InitializeParameters(*engineA.database.instance, s.u1); },
                     "database+path entry point");
            runEntry([&]() { return wrapper.InitializeParameters(*engineA.connection.context, s.u1); },
                     "context+path entry point");
            std::printf("PASS case6 wrapper entrypoints use private copies of captured signed parameters (parameter factory model)\n");
        }
        // Mixed local: valid raw parquet exists, but only a verified read-only
        // descriptor is admitted; FD lifetimes are RAII on every failure exit.
        {
            Require(grv_native_gcs_metadata_mode(s.ownerA,nullptr,0)==0,"metadata mode");
            FD fd(::open(s.localParquet.c_str(),O_RDONLY));
            Require(::unlink(s.localParquet.c_str())==0,"unlink fixture");
            char error[4096]={0}; unsigned char output[65536]; size_t written=0;
            auto sql="SELECT sum(value)::BIGINT FROM read_parquet('/dev/fd/"+std::to_string(fd.value)+
                     "', hive_partitioning=false, union_by_name=false)";
            Require(grv_native_verified_file_query(s.ownerA,fd.value,sql.c_str(),output,sizeof(output),&written,error,sizeof(error))==0,"verified FD query");
            auto row=Decode(output,written);
            Require(row.rows==1 && row.cols==1 && row.valid[0][0] && row.cells[0][0]=="37","verified FD oracle");
            auto raw=s.root+"/raw.parquet";
            {std::ofstream file(raw,std::ios::binary); file.write(bytes37.data(),bytes37.size()); Require(bool(file),"raw fixture write");}
            GCSScanScope scope7(engineA,{s.u1});
            refused(engineA,"SELECT * FROM read_parquet('"+raw+"')");
            FD writable(::open(raw.c_str(),O_RDWR));
            Require(grv_native_verified_file_query(s.ownerA,writable.value,"SELECT 1",output,sizeof(output),&written,error,sizeof(error))!=0,"writable FD admitted");
            FD dirfd(::open(s.root.c_str(),O_RDONLY));
            Require(grv_native_verified_file_query(s.ownerA,dirfd.value,"SELECT 1",output,sizeof(output),&written,error,sizeof(error))!=0,"directory FD admitted");
            std::printf("PASS case7 verified/unlinked FD; valid raw local, writable and directory denial\n");
        }
        // Independent synthetic invocations may carry the same token value.
        // No provider acquisition is performed by this proof.
        {
            ConfigureOwner(&s, s.ownerE, extensions, s.e, s.mockE);
            auto &engineE = *static_cast<Owner *>(s.ownerE)->engine;
            {
                GCSScanScope scopeE(engineE, {s.u1});
                auto result = engineE.connection.Query(scan(s.u1));
                auto eRow = ReadFirstRow(result);
                Require(!result->HasError() && !eRow.values.empty() && eRow.values[0].GetValue<int64_t>() == 111,
                        "owner E scan failed: " + result->GetError());
            }
            // A and E hold the SAME token value; each resolves its own secret
            // and only its own mock sees its own dispatch.
            GCSScanScope scopeA2(engineA, {s.u1});
            auto aResult = engineA.connection.Query(scan(s.u1));
            Require(!aResult->HasError(), "owner A re-scan failed: " + aResult->GetError());
            for (const auto &record : s.mockE.records) {
                Require(record.bearer == s.a.canary, "owner E dispatch carried a foreign bearer");
            }
            for (const auto &record : s.mockA.records) {
                Require(record.bearer == s.a.canary, "owner A dispatch carried a foreign bearer");
            }
            std::printf("PASS case11c independent synthetic invocations with identical token value\n");
        }
        // --- case 11-1: bind-level refusal does not poison the owner ----------------
        {
            char error[4096] = {0};
            unsigned char output[4096];
            size_t written = 0;
            const char *bad[] = {"https://storage.googleapis.com/Bad_Bucket/data/x.parquet"};
            Require(grv_native_gcs_query(s.ownerA, bad, 1, "SELECT 1", output, sizeof(output), &written, error,
                                          sizeof(error)) != 0,
                    "malformed-URL scope was admitted");
            Require(string(error).find(s.a.canary) == string::npos, "bind refusal carried a canary");
            // The owner is NOT poisoned by a bind refusal: a valid scope works.
            GCSScanScope scope8(engineA, {s.u1});
            auto ok = engineA.connection.Query(scan(s.u1));
            auto okRow = ReadFirstRow(ok);
            Require(!ok->HasError() && !okRow.values.empty() && okRow.values[0].GetValue<int64_t>() == 111,
                    "owner was poisoned by a bind-level refusal");
            std::printf("PASS case11a bind-level refusal isolates: owner stays usable, no canary leak\n");
        }
        // --- case 11-2: parallel two-owner transport-failure isolation ----------------
        {
            ConfigureOwner(&s, s.ownerD, extensions, s.d, s.mockD);
            auto &engineD = *static_cast<Owner *>(s.ownerD)->engine;
            s.mockA.fail_head.insert(s.u1); // owner A's next HEAD fails
            std::atomic<int> started{0};
            std::atomic<bool> aDone{false}, dDone{false};
            std::atomic<int> aStatus{-99}, dStatus{-99};
            char aError[4096] = {0}, dError[4096] = {0};
            unsigned char aOutput[65536], dOutput[65536];
            size_t aWritten = 0, dWritten = 0;
            auto workerA = [&]() {
                started.fetch_add(1, std::memory_order_acq_rel);
                auto until = std::chrono::steady_clock::now() + std::chrono::seconds(30);
                while (started.load(std::memory_order_acquire) < 2 && std::chrono::steady_clock::now() < until) {
                    std::this_thread::yield();
                }
                string sql = scan(s.u1);
                const char *urls[] = {s.u1.c_str()};
                aStatus = grv_native_gcs_query(s.ownerA, urls, 1, sql.c_str(), aOutput, sizeof(aOutput), &aWritten,
                                                aError, sizeof(aError));
                aDone.store(true);
            };
            auto workerD = [&]() {
                started.fetch_add(1, std::memory_order_acq_rel);
                auto until = std::chrono::steady_clock::now() + std::chrono::seconds(30);
                while (started.load(std::memory_order_acquire) < 2 && std::chrono::steady_clock::now() < until) {
                    std::this_thread::yield();
                }
                string sql = scan(s.uD1);
                const char *urls[] = {s.uD1.c_str()};
                dStatus = grv_native_gcs_query(s.ownerD, urls, 1, sql.c_str(), dOutput, sizeof(dOutput), &dWritten,
                                                dError, sizeof(dError));
                dDone.store(true);
            };
            JoinedWorker threadA(workerA), threadD(workerD);
            bool within=Wait([&](){return aDone.load()&&dDone.load();},std::chrono::seconds(60));
            threadA.Join(); threadD.Join();
            threadA.Rethrow(); threadD.Rethrow();
            Require(within,"parallel fixture did not finish within observed bound");
            Require(aStatus != 0, "owner A scan with the injected HEAD failure succeeded");
            Require(dStatus == 0, "owner D scan was falsely failed by owner A's failure");
            auto decodedD = Decode(dOutput, dWritten);
            Require(decodedD.rows == 1 && decodedD.valid[0][0] && decodedD.cells[0][0] == "777",
                    "owner D scan returned the wrong value");
            Require(string(aError).find(s.a.canary) == string::npos && string(aError).find(s.d.canary) == string::npos,
                    "failure error carried a canary");
            // No cross-scope data: D's mock only saw D's URL with D's canary.
            for (const auto &record : s.mockD.records) {
                Require(record.url == s.uD1 && record.bearer == s.d.canary, "owner D mock saw a foreign record");
            }
            for (const auto &record : s.mockA.records) {
                Require(record.bearer == s.a.canary, "owner A mock saw a foreign bearer");
            }
            // A is poisoned fail-closed; D is not.
            Require(engineA.exact_gcs->failed, "owner A was not poisoned by its transport failure");
            Require(!engineD.exact_gcs->failed, "owner D was poisoned by owner A's failure");
            // Client disposal was witnessed on both sides (non-vacuous).
            Require(s.mockA.clients_created >= 1 && s.mockA.clients_created == s.mockA.clients_destroyed,
                    "owner A request clients were not all disposed");
            Require(s.mockD.clients_created >= 1 && s.mockD.clients_created == s.mockD.clients_destroyed,
                    "owner D request clients were not all disposed");
            // D's next scope still works (no false failure, no old authority
            // granted to A).
            {
                GCSScanScope scopeD2(engineD, {s.uD1});
                auto ok = engineD.connection.Query(scan(s.uD1));
                auto okRow = ReadFirstRow(ok);
                Require(!ok->HasError() && !okRow.values.empty() && okRow.values[0].GetValue<int64_t>() == 777,
                        "owner D lost authority after owner A's unrelated failure");
            }
            std::printf("PASS case11b parallel two-owner isolation: A's injected failure never touched D\n");
        }
        // GET exception and body rejection are separate fresh-owner cases.
        for(int variant=0;variant<2;++variant) {
            auto &canary=variant?s.f:s.b; auto &mock=variant?s.mockF:s.mockB;
            auto &owner=variant?s.ownerF:s.ownerB;
            ConfigureOwner(&s,owner,extensions,canary,mock);
            auto &engineB = *static_cast<Owner *>(owner)->engine;
            auto &policyB = *GRVGuard::Get(*engineB.connection.context);
            size_t policyTablesBefore = policyB.tables.size();
            auto pathsBefore = DBConfig::GetConfig(*engineB.connection.context).options.allowed_paths;
            string bUrl = canary.scope + "/object-b1.parquet";
            mock.content[bUrl] = bytes222;
            if(variant)mock.reject_content.insert(bUrl);else mock.fail_get.insert(bUrl);
            char error[4096] = {0};
            unsigned char output[65536];
            size_t written = 0;
            const char *bUrls[] = {bUrl.c_str()};
            auto bScan = "SELECT id FROM read_parquet('" + bUrl + "', hive_partitioning=false, union_by_name=false)";
            int status = grv_native_gcs_query(owner, bUrls, 1, bScan.c_str(), output, sizeof(output), &written, error,
                                               sizeof(error));
            Require(status != 0, "scan with the injected GET failure succeeded");
            Require(string(error).find(canary.canary) == string::npos, "failure error carried a canary");
            // Cleanup joined: the policy tables and allowed paths are exactly
            // restored; the request authority is inactive.
            Require(policyB.tables.size() == policyTablesBefore, "scanner callbacks were not restored");
            Require(DBConfig::GetConfig(*engineB.connection.context).options.allowed_paths == pathsBefore,
                    "allowed paths were not restored");
            Require(!engineB.exact_gcs->active, "scope was left active after the failure");
            Require(engineB.exact_gcs->admitted.empty(), "admission was not cleared after the failure");
            Require(engineB.exact_gcs->failed, "transport failure did not poison the owner fail-closed");
            // No fresh owner authority from the old params: a new scope is
            // refused while the failed state stands.
            char error2[4096] = {0};
            size_t written2 = 0;
            unsigned char output2[4096];
            Require(grv_native_gcs_query(owner, bUrls, 1, "SELECT 1", output2, sizeof(output2), &written2, error2,
                                          sizeof(error2)) != 0,
                    "a new scope was admitted on a failed owner");
            // Local work remains available (proved-failure semantics).
            auto local = engineB.connection.Query("SELECT 42");
            auto localRow = ReadFirstRow(local);
            Require(!local->HasError() && !localRow.values.empty() && localRow.values[0].GetValue<int64_t>() == 42,
                    "local work was unavailable after a proved cleanup failure");
            // Client disposal was witnessed (non-vacuous): the RAII dispose ran
            // even though the stock delegate threw.
            Require(mock.clients_created >= 1 && mock.clients_created == mock.clients_destroyed,
                    "request clients were not all disposed after the failure");
            std::printf("PASS case8 variant=%d failure cleanup/restoration/disposal\n",variant);
        }
        // --- case 9: cooperative cancel + owner close/read-only reopen ---------------
        {
            ConfigureOwner(&s,s.ownerC,extensions,s.c,s.mockC);
            auto &engineC = *static_cast<Owner *>(s.ownerC)->engine;
            GRVGuard::Get(*engineC.connection.context)->relations.insert("main._grv_cancel_witness");
            query(engineC,"CREATE TABLE _grv_cancel_witness AS SELECT 991::BIGINT AS id");
            auto &policyC = *GRVGuard::Get(*engineC.connection.context);
            size_t policyTablesBefore = policyC.tables.size();
            auto pathsBefore = DBConfig::GetConfig(*engineC.connection.context).options.allowed_paths;
            string cUrl = s.c.scope + "/object-c1.parquet";
            s.mockC.content[cUrl] = bytes888;
            s.mockC.gate_url = cUrl;
            s.mockC.gate_armed = true;
            s.mockC.gate_context = engineC.connection.context.get();
            InterruptRef interruptHandle(s.ownerC);
            char error[4096] = {0};
            unsigned char output[65536];
            size_t written = 0;
            const char *cUrls[] = {cUrl.c_str()};
            std::atomic<bool> scanDone{false};
            std::atomic<int> scanStatus{-99};
            auto worker = [&]() {
                // The scan blocks in the mock GET gate; the range sum after it
                // gives the executor a long CPU phase for the interrupt to land.
                string sql = "SELECT (SELECT count(*) FROM read_parquet('" + cUrl +
                              "', hive_partitioning=false, union_by_name=false)) + (SELECT sum(i::BIGINT) FROM "
                              "range(1000000000) r(i))";
                scanStatus = grv_native_gcs_query(s.ownerC, cUrls, 1, sql.c_str(), output, sizeof(output), &written,
                                                   error, sizeof(error));
                scanDone.store(true);
            };
            JoinedWorker thread(worker,[&](){s.mockC.ReleaseGate();grv_native_interrupt(interruptHandle.value);});
            bool entered=Wait([&](){return s.mockC.in_gate.load();},std::chrono::seconds(30));
            if(entered)grv_native_interrupt(interruptHandle.value);else s.mockC.ReleaseGate();
            bool within=Wait([&](){return scanDone.load();},std::chrono::seconds(30));
            thread.Join(); thread.Rethrow();
            Require(entered,"scan never entered gated GET");
            Require(within,"cancelled fixture scan did not finish within observed bound");
            Require(scanStatus.load() != 0, "interrupted scan reported success");
            Require(string(error).find(s.c.canary) == string::npos, "cancel error carried a canary");
            // The interrupt is a request, not a stop: the owner is poisoned
            // fail-closed, the cleanup witness ran, and the state is restored.
            Require(engineC.exact_gcs->failed, "cancelled scan did not poison the owner fail-closed");
            Require(!engineC.exact_gcs->active, "scope was left active after the cancel");
            Require(engineC.exact_gcs->admitted.empty(), "admission was not cleared after the cancel");
            Require(policyC.tables.size() == policyTablesBefore, "scanner callbacks were not restored");
            Require(DBConfig::GetConfig(*engineC.connection.context).options.allowed_paths == pathsBefore,
                    "allowed paths were not restored");
            Require(s.mockC.clients_created >= 1 && s.mockC.clients_created == s.mockC.clients_destroyed,
                    "request clients were not all disposed after the cancel");
            // Synchronous owner-thread close, then a fresh read-only reopen
            // under the same workspace: durable local state survives, no
            // GCS authority is inherited.
            grv_native_close(s.ownerC);
            s.ownerC = nullptr;
            char error2[4096] = {0};
            s.ownerC = grv_native_open_readonly((s.root + "/project-c.duckdb").c_str(), error2, sizeof(error2));
            Require(s.ownerC != nullptr, "read-only reopen failed: " + string(error2));
            auto &engineR = *static_cast<Owner *>(s.ownerC)->engine;
            GRVGuard::Get(*engineR.connection.context)->relations.insert("main._grv_cancel_witness");
            auto local = engineR.connection.Query("SELECT id FROM _grv_cancel_witness");
            auto localRow = ReadFirstRow(local);
            Require(!local->HasError() && localRow.rows==1 && localRow.values[0].GetValue<int64_t>()==991,"durable witness did not survive");
            Require(DBConfig::GetConfig(*engineR.connection.context).options.access_mode==AccessMode::READ_ONLY,"reopen not read-only");
            Require(engineR.connection.Query("INSERT INTO _grv_cancel_witness VALUES (992)")->HasError(),"read-only write accepted");
            Require(!engineR.exact_gcs->configured, "reopened owner inherited GCS configuration");
            grv_native_close(s.ownerC); s.ownerC=nullptr;
            std::printf("PASS case9 cooperative cancel: request-not-stop, fail-closed, restored, disposed, read-only reopen\n");
        }
        // Every recorded physical request must have used the actual wrapper
        // SendRequest delegate, not the supplementary/direct-client path.
        for(size_t i=0;i<6;++i) {
            MockStock *mocks[]={&s.mockA,&s.mockB,&s.mockC,&s.mockD,&s.mockE,&s.mockF};
            const OwnerCanary *owners[]={&s.a,&s.b,&s.c,&s.d,&s.e,&s.f};
            auto *mock=mocks[i]; const auto &owner=*owners[i];
            Require(!mock->records.empty(),"vacuous owner transport record");
            Require(mock->clients_created>0 && mock->clients_created==mock->clients_destroyed,"owner client disposal");
            for(const auto &record:mock->records) {
                Require(record.via_wrapper,"physical request bypassed wrapper");
                Require(record.bearer==owner.canary && record.project==owner.project,"cross-owner credential/routing contamination");
                Require(StringUtil::StartsWith(record.url,owner.scope+"/"),"cross-owner data route");
                Require(record.timeout<=30 && record.timeout_usec<1000000 &&
                        (record.timeout>0 || record.timeout_usec>0) &&
                        (record.timeout<30 || record.timeout_usec==0),"request timeout not shortened");
                Require(!record.follow_location && record.retries==0,"request safety options");
            }
            mock->captured.reset();
        }
        retained.reset(); retainedPrepared.reset();
        s.CloseOwners();
        for(const auto &owner:{s.a,s.b,s.c,s.d,s.e,s.f}) {
            auto db=s.root+"/"+owner.project+".duckdb"; struct stat st;
            Require(::lstat(db.c_str(),&st)==0 && S_ISREG(st.st_mode),"required database artifact absent");
        }
        CanaryFreeArtifacts(s.root,{s.a.canary,s.b.canary,s.c.canary,s.d.canary,s.e.canary,s.f.canary});
        RemoveTree(s.root);
        struct stat absent; errno=0;
        Require(::lstat(s.root.c_str(),&absent)!=0 && errno==ENOENT,"owned cleanup incomplete");
        s.root.clear();
        std::printf("PASS closed/flushed owned artifact scans and verified local absence\n");
        std::printf("gcs scanner lifecycle proof passed\n");
        return 0;
    } catch (const std::exception &failure) {
        std::fprintf(stderr, "GCS scanner lifecycle proof failed: %s\n", failure.what());
    }
    return 1;
}
