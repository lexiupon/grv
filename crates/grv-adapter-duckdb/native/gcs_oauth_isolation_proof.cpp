// OAuth isolation proof: two independent native owners, each with its own
// synthetic bearer canary, prove per-owner secret isolation, exact per-scope
// routing through the real signed-extension secret resolution, and refusal of
// cross-scope, non-admitted, write, expired and introspection paths before
// any dispatch. Source-including proof only (includes shim.cpp); it is never
// compiled into the shipping library.
//
// Transport sections: the policy-wrapper section drives the ACTUAL shipping
// wrapper grv::GCSHTTPPolicy::SendRequest (private parameter copy, boundary,
// client disposal) with a mock transport recorder: no network, no cloud, no
// credential beyond the synthetic canaries. The supplementary
// GRVHTTPRequestGuard-registry section exercises the patched base-Request hook
// only; the signed extension's private request path is known to bypass that
// registry, so that section is labelled supplementary, not the wrapper proof.
// The boundary is admission-based (scope/deadline/read-only); it does not
// re-verify the token, because in the real flow parameters are only ever
// created by the owner's own policy InitializeParameters (own-secret
// resolution), so a foreign or forged bearer cannot enter that flow.
#include "shim.cpp"
#include "duckdb/main/settings.hpp"
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <future>
#include <thread>
#include <sys/stat.h>
namespace {
struct CanaryOwners {
    string canaryA="synthetic-canary-owner-a-not-a-credential";
    string canaryB="synthetic-canary-owner-b-not-a-credential";
    string scopeA="https://storage.googleapis.com/owner-a-bucket/data";
    string scopeB="https://storage.googleapis.com/owner-b-bucket/data";
    string urlA="https://storage.googleapis.com/owner-a-bucket/data/object.parquet";
    string urlB="https://storage.googleapis.com/owner-b-bucket/data/object.parquet";
    string otherA="https://storage.googleapis.com/owner-a-bucket/data/other.parquet";
};
// Test-only transport recorder; SendRequest is invoked by the policy wrapper
// (or the patched base Request in the supplementary section) after the
// boundary has run.
struct MockStock final : HTTPUtil {
    struct TrackedClient final : HTTPClient {
        int &destroyed;
        explicit TrackedClient(int &destroyed):destroyed(destroyed){}
        ~TrackedClient() override {++destroyed;}
        void Initialize(HTTPParams &) override {}
        unique_ptr<HTTPResponse> Get(GetRequestInfo &) override {return nullptr;}
        unique_ptr<HTTPResponse> Put(PutRequestInfo &) override {return nullptr;}
        unique_ptr<HTTPResponse> Head(HeadRequestInfo &) override {return nullptr;}
        unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &) override {return nullptr;}
        unique_ptr<HTTPResponse> Post(PostRequestInfo &) override {return nullptr;}
    };
    int clients_created=0,clients_destroyed=0;
    struct Record {
        string url,bearer,project;
        uint64_t timeout=0,timeout_usec=0;
        bool follow_location=false;
        idx_t retries=0;
    };
    vector<Record> records;
    unique_ptr<HTTPResponse> SendRequest(BaseRequest &request,unique_ptr<HTTPClient> &client) override {
        client=make_uniq<TrackedClient>(clients_destroyed);
        ++clients_created;
        auto &params=request.params.Cast<HTTPFSParams>();
        string project;
        auto found=params.extra_headers.find("x-goog-user-project");
        if(found!=params.extra_headers.end())project=found->second;
        records.push_back({request.url,params.bearer_token,project,params.timeout,params.timeout_usec,params.follow_location,params.retries});
        return make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
    }
};
string SecretValue(Engine &engine) {
    string value;
    engine.connection.context->RunFunctionInTransaction([&](){
        auto transaction=CatalogTransaction::GetSystemCatalogTransaction(*engine.connection.context);
        auto entry=SecretManager::Get(*engine.connection.context).GetSecretByName(transaction,"_grv_exact_gcs_reader","memory");
        if(!entry||!entry->secret)throw IOException("setup secret absent from memory storage");
        auto &secret=*entry->secret;
        if(secret.GetType()!="http")throw IOException("setup secret is not an http secret");
        value=secret.ToString(SecretDisplayType::UNREDACTED);
    });
    return value;
}
// Owned-artifact scan: a stat failure on an artifact we created (EACCES and
// friends) fails the proof; only an optional WAL that was never created
// (ENOENT) is skipped. lstat never follows symlinks.
void ScanOwnedArtifacts(const vector<string> &paths,CanaryOwners &canaries) {
    for(const auto &path:paths) {
        struct stat st{};
        if(::lstat(path.c_str(),&st)!=0) {
            if(errno==ENOENT && StringUtil::EndsWith(path, ".wal"))continue;
            throw IOException("owned artifact stat failed: "+path);
        }
        if(!S_ISREG(st.st_mode))throw IOException("owned artifact is not a regular file: "+path);
        std::ifstream input(path,std::ios::binary);
        if(!input)throw IOException("durable artifact unreadable: "+path);
        string bytes((std::istreambuf_iterator<char>(input)),std::istreambuf_iterator<char>());
        if(input.bad() || st.st_size < 0 || bytes.size()!=static_cast<size_t>(st.st_size))throw IOException("owned artifact read incomplete");
        if(bytes.find(canaries.canaryA)!=string::npos||bytes.find(canaries.canaryB)!=string::npos) {
            throw IOException("setup canary found in durable artifact: "+path);
        }
    }
}
struct OwnerCleanup {
    void *&a;
    void *&b;
    void Close() {
        if(a){grv_native_close(a);a=nullptr;}
        if(b){grv_native_close(b);b=nullptr;}
    }
    ~OwnerCleanup(){Close();}
};
struct WorkspaceCleanup {
    string root,dirA,dirB,dbA,dbB;
    ~WorkspaceCleanup() {
        for(const auto &path:{dbA,dbA+".wal",dbB,dbB+".wal"}) {
            struct stat st{};
            if(::lstat(path.c_str(),&st)==0&&S_ISREG(st.st_mode))::unlink(path.c_str());
        }
        ::rmdir(dirA.c_str());::rmdir(dirB.c_str());::rmdir(root.c_str());
    }
};
}
int main(int argc,char **argv) {
    if(argc!=2)return 2;
    CanaryOwners canaries;
    char pattern[]="/tmp/grv-gcs-oauth-isolation-XXXXXX";
    auto *base=::mkdtemp(pattern);
    if(!base)return 2;
    string rootPath=base;
    auto dirA=rootPath+"/a";auto dirB=rootPath+"/b";
    string dbA=dirA+"/owner.duckdb",dbB=dirB+"/owner.duckdb";
    WorkspaceCleanup workspace{rootPath,dirA,dirB,dbA,dbB};
    void *ownerA=nullptr,*ownerB=nullptr;
    OwnerCleanup owners{ownerA,ownerB};
    try {
        ::mkdir(dirA.c_str(),0700);::mkdir(dirB.c_str(),0700);
        char errorA[4096]={0},errorB[4096]={0};
        ownerA=grv_native_open(dbA.c_str(),errorA,sizeof(errorA));
        ownerB=grv_native_open(dbB.c_str(),errorB,sizeof(errorB));
        if(!ownerA||!ownerB)throw IOException("owner");
        auto &engineA=*static_cast<Owner *>(ownerA)->engine;
        auto &engineB=*static_cast<Owner *>(ownerB)->engine;
        if(grv_native_gcs_configure(ownerA,argv[1],canaries.scopeA.c_str(),canaries.canaryA.c_str(),"project-a",300000,errorA,sizeof(errorA))!=0)throw IOException("configure A: "+string(errorA));
        if(grv_native_gcs_configure(ownerB,argv[1],canaries.scopeB.c_str(),canaries.canaryB.c_str(),"project-b",300000,errorB,sizeof(errorB))!=0)throw IOException("configure B: "+string(errorB));
        if(string(errorA).find(canaries.canaryA)!=string::npos||string(errorB).find(canaries.canaryB)!=string::npos)throw IOException("setup error carried a canary");
        // 1) Secret-value isolation: each owner's memory secret holds exactly
        //    its own canary; the other owner's canary is not present.
        auto valueA=SecretValue(engineA);
        auto valueB=SecretValue(engineB);
        if(valueA.find(canaries.canaryA)==string::npos)throw IOException("owner A secret lost its own canary");
        if(valueA.find(canaries.canaryB)!=string::npos)throw IOException("owner A secret retained owner B canary");
        if(valueB.find(canaries.canaryB)==string::npos)throw IOException("owner B secret lost its own canary");
        if(valueB.find(canaries.canaryA)!=string::npos)throw IOException("owner B secret retained owner A canary");
        // 2) Exact routing: the installed policy's stock resolution maps each
        //    in-scope URL to exactly its owner's canary and project header.
        auto &configA=DBConfig::GetConfig(*engineA.connection.context);
        auto &configB=DBConfig::GetConfig(*engineB.connection.context);
        auto &policyA=configA.GetHTTPUtil();
        auto &policyB=configB.GetHTTPUtil();
        unique_ptr<HTTPParams> paramsA;
        engineA.connection.context->RunFunctionInTransaction([&](){paramsA=policyA.InitializeParameters(*engineA.connection.context,canaries.urlA);});
        unique_ptr<HTTPParams> paramsB;
        engineB.connection.context->RunFunctionInTransaction([&](){paramsB=policyB.InitializeParameters(*engineB.connection.context,canaries.urlB);});
        auto &fsA=paramsA->Cast<HTTPFSParams>();
        auto &fsB=paramsB->Cast<HTTPFSParams>();
        if(fsA.bearer_token!=canaries.canaryA)throw IOException("owner A parameters did not resolve its own canary");
        if(fsB.bearer_token!=canaries.canaryB)throw IOException("owner B parameters did not resolve its own canary");
        auto projectA=fsA.extra_headers.find("x-goog-user-project");
        auto projectB=fsB.extra_headers.find("x-goog-user-project");
        if(projectA==fsA.extra_headers.end()||projectA->second!="project-a")throw IOException("owner A project routing header missing");
        if(projectB==fsB.extra_headers.end()||projectB->second!="project-b")throw IOException("owner B project routing header missing");
        // Cross-owner resolution: B's policy asked for A's scope resolves no
        // bearer at all; no cross-owner token is ever produced.
        unique_ptr<HTTPParams> paramsCross;
        engineB.connection.context->RunFunctionInTransaction([&](){paramsCross=policyB.InitializeParameters(*engineB.connection.context,canaries.urlA);});
        if(!paramsCross->Cast<HTTPFSParams>().bearer_token.empty())throw IOException("cross-owner secret resolution produced a bearer");
        // 3) Policy-wrapper boundary: the ACTUAL shipping wrapper
        //    grv::GCSHTTPPolicy::SendRequest (private parameter copy,
        //    boundary, client disposal) with a mock transport. The global
        //    guard registry is not touched by this path.
        auto stateA=engineA.exact_gcs;
        auto stateB=engineB.exact_gcs;
        stateA->admitted.insert(canaries.urlA);stateA->active=true;
        stateB->admitted.insert(canaries.urlB);stateB->active=true;
        {
            // In-scope dispatch: the boundary passes and the recorded request
            // carries exactly owner A's canary and project routing; the
            // original shared parameters remain untouched.
            MockStock stockA;
            auto wrapper=make_shared_ptr<grv::GCSHTTPPolicy>(stockA,"project-a",[&stateA](BaseRequest &r,unique_ptr<HTTPClient> &c){stateA->BeforeRequest(r,c);});
            HTTPHeaders headers;
            HeadRequestInfo request(canaries.urlA,headers,*paramsA);
            unique_ptr<HTTPClient> client;
            auto response=wrapper->SendRequest(request,client);
            if(!response||response->status!=HTTPStatusCode::OK_200)throw IOException("in-scope dispatch refused by mock transport");
            if(stockA.records.size()!=1)throw IOException("in-scope dispatch was not recorded exactly once");
            if(stockA.records[0].bearer!=canaries.canaryA)throw IOException("dispatched request carried the wrong canary");
            if(stockA.records[0].project!="project-a")throw IOException("dispatched request carried the wrong project routing");
            if(stockA.records[0].timeout==0||stockA.records[0].timeout>30)throw IOException("dispatched request timeout not bounded");
            if(stockA.records[0].follow_location)throw IOException("dispatched request retained redirect following");
            if(stockA.records[0].retries!=0)throw IOException("dispatched request retained retries");
            // The private copy was zeroized on disposal; the original shared
            // parameters keep their own (owner-resolved) values.
            if(paramsA->Cast<HTTPFSParams>().bearer_token!=canaries.canaryA)throw IOException("original shared parameters were mutated by the wrapper");
        }
        {
            MockStock stockB;
            auto wrapper=make_shared_ptr<grv::GCSHTTPPolicy>(stockB,"project-b",[&stateB](BaseRequest &r,unique_ptr<HTTPClient> &c){stateB->BeforeRequest(r,c);});
            HTTPHeaders headers;
            HeadRequestInfo request(canaries.urlB,headers,*paramsB);
            unique_ptr<HTTPClient> client;
            auto response=wrapper->SendRequest(request,client);
            if(!response||response->status!=HTTPStatusCode::OK_200)throw IOException("in-scope dispatch refused by mock transport");
            if(stockB.records.size()!=1||stockB.records[0].bearer!=canaries.canaryB||stockB.records[0].project!="project-b")throw IOException("owner B dispatch did not carry its own canary");
        }
        {
            // Cross-scope: B's wrapper, A's scope. Refused at the boundary
            // before any dispatch; the error carries no canary.
            MockStock stockB;
            auto wrapper=make_shared_ptr<grv::GCSHTTPPolicy>(stockB,"project-b",[&stateB](BaseRequest &r,unique_ptr<HTTPClient> &c){stateB->BeforeRequest(r,c);});
            HTTPHeaders headers;
            HeadRequestInfo request(canaries.urlA,headers,*paramsCross);
            unique_ptr<HTTPClient> client;
            string error;
            try {wrapper->SendRequest(request,client);}
            catch(const std::exception &failure){error=failure.what();}
            if(error.empty())throw IOException("cross-scope request was not refused");
            if(error.find(canaries.canaryA)!=string::npos||error.find(canaries.canaryB)!=string::npos)throw IOException("cross-scope error carried a canary");
            if(!stockB.records.empty())throw IOException("cross-scope request reached the transport");
            // The reverse direction is refused the same way.
            MockStock stockA;
            auto wrapperA=make_shared_ptr<grv::GCSHTTPPolicy>(stockA,"project-a",[&stateA](BaseRequest &r,unique_ptr<HTTPClient> &c){stateA->BeforeRequest(r,c);});
            unique_ptr<HTTPParams> paramsReverse;
            engineA.connection.context->RunFunctionInTransaction([&](){paramsReverse=policyA.InitializeParameters(*engineA.connection.context,canaries.urlB);});
            HeadRequestInfo reverse(canaries.urlB,headers,*paramsReverse);
            unique_ptr<HTTPClient> clientA;
            try {wrapperA->SendRequest(reverse,clientA);throw IOException("reverse cross-scope request was not refused");}
            catch(const PermissionException &){}
            if(!stockA.records.empty())throw IOException("reverse cross-scope request reached the transport");
        }
        {
            // Non-admitted in-scope URL: the scope prefix alone admits no
            // request; exact per-scan admission is required.
            MockStock stockA;
            auto wrapper=make_shared_ptr<grv::GCSHTTPPolicy>(stockA,"project-a",[&stateA](BaseRequest &r,unique_ptr<HTTPClient> &c){stateA->BeforeRequest(r,c);});
            unique_ptr<HTTPParams> paramsOther;
            engineA.connection.context->RunFunctionInTransaction([&](){paramsOther=policyA.InitializeParameters(*engineA.connection.context,canaries.otherA);});
            HTTPHeaders headers;
            HeadRequestInfo request(canaries.otherA,headers,*paramsOther);
            unique_ptr<HTTPClient> client;
            try {wrapper->SendRequest(request,client);throw IOException("non-admitted request was not refused");}
            catch(const PermissionException &){}
            if(!stockA.records.empty())throw IOException("non-admitted request reached the transport");
        }
        {
            // Write requests are refused at the boundary regardless of scope.
            MockStock stockA;
            auto wrapper=make_shared_ptr<grv::GCSHTTPPolicy>(stockA,"project-a",[&stateA](BaseRequest &r,unique_ptr<HTTPClient> &c){stateA->BeforeRequest(r,c);});
            HTTPHeaders headers;
            PutRequestInfo request(canaries.urlA,headers,*paramsA,nullptr,0,"application/octet-stream");
            unique_ptr<HTTPClient> client;
            try {wrapper->SendRequest(request,client);throw IOException("write request was not refused");}
            catch(const PermissionException &){}
            if(!stockA.records.empty())throw IOException("write request reached the transport");
        }
        {
            // Expired deadline: the boundary refuses even an admitted URL.
            auto deadline=stateA->deadline;
            stateA->deadline=std::chrono::steady_clock::now()-std::chrono::milliseconds(1);
            MockStock stockA;
            auto wrapper=make_shared_ptr<grv::GCSHTTPPolicy>(stockA,"project-a",[&stateA](BaseRequest &r,unique_ptr<HTTPClient> &c){stateA->BeforeRequest(r,c);});
            HTTPHeaders headers;
            HeadRequestInfo request(canaries.urlA,headers,*paramsA);
            unique_ptr<HTTPClient> client;
            try {wrapper->SendRequest(request,client);throw IOException("expired request was not refused");}
            catch(const PermissionException &){}
            if(!stockA.records.empty())throw IOException("expired request reached the transport");
            stateA->deadline=deadline;
        }
        {
            // Concurrent dispatch: two threads, each exclusively owning its
            // policy wrapper, mock stock, parameters, state and client. The
            // global guard registry is not mutated by this path.
            MockStock stockA;
            auto wrapperA=make_shared_ptr<grv::GCSHTTPPolicy>(stockA,"project-a",[&stateA](BaseRequest &r,unique_ptr<HTTPClient> &c){stateA->BeforeRequest(r,c);});
            MockStock stockB;
            auto wrapperB=make_shared_ptr<grv::GCSHTTPPolicy>(stockB,"project-b",[&stateB](BaseRequest &r,unique_ptr<HTTPClient> &c){stateB->BeforeRequest(r,c);});
            const auto deadlineA=stateA->deadline,deadlineB=stateB->deadline;
            const auto timeoutA=paramsA->timeout,timeoutB=paramsB->timeout;
            const auto usecA=paramsA->timeout_usec,usecB=paramsB->timeout_usec;
            const auto headersA=paramsA->extra_headers,headersB=paramsB->extra_headers;
            stateA->deadline=stateB->deadline=std::chrono::steady_clock::now()+std::chrono::seconds(10);
            std::atomic<int> started{0};
            auto worker=[&](auto &wrapper,HTTPParams &params,const string &url){
                started.fetch_add(1, std::memory_order_acq_rel);
                const auto until=std::chrono::steady_clock::now()+std::chrono::seconds(10);
                while(started.load(std::memory_order_acquire)<2){
                    if(std::chrono::steady_clock::now()>=until)throw IOException("concurrent rendezvous timed out");
                    std::this_thread::yield();
                }
                HTTPHeaders headers;
                HeadRequestInfo request(url,headers,params);
                unique_ptr<HTTPClient> client;
                auto response=wrapper->SendRequest(request,client);
                if(!response||response->status!=HTTPStatusCode::OK_200)throw IOException("concurrent dispatch refused by mock transport");
                if(client)throw IOException("request client survived wrapper dispatch");
            };
            // Futures propagate exceptions and join even if one launch/call
            // fails; the bounded rendezvous cannot strand the other worker.
            auto left=std::async(std::launch::async,[&]{worker(wrapperA,*paramsA,canaries.urlA);});
            auto right=std::async(std::launch::async,[&]{worker(wrapperB,*paramsB,canaries.urlB);});
            left.get();right.get();
            if(stockA.records.size()!=1||stockA.records[0].bearer!=canaries.canaryA||stockA.records[0].project!="project-a")throw IOException("concurrent owner A dispatch did not carry its own canary");
            if(stockB.records.size()!=1||stockB.records[0].bearer!=canaries.canaryB||stockB.records[0].project!="project-b")throw IOException("concurrent owner B dispatch did not carry its own canary");
            if(stockA.clients_created!=1 || stockA.clients_destroyed!=1 || stockB.clients_created!=1 || stockB.clients_destroyed!=1)throw IOException("per-request client destruction not witnessed");
            if(paramsA->timeout!=timeoutA || paramsA->timeout_usec!=usecA || paramsA->extra_headers!=headersA || paramsB->timeout!=timeoutB || paramsB->timeout_usec!=usecB || paramsB->extra_headers!=headersB)throw IOException("wrapper mutated shared timeout or header parameters");
            stateA->deadline=deadlineA;stateB->deadline=deadlineB;
            // Original shared parameters survive the concurrent private copies.
            if(paramsA->Cast<HTTPFSParams>().bearer_token!=canaries.canaryA)throw IOException("concurrent wrapper mutated owner A shared parameters");
            if(paramsB->Cast<HTTPFSParams>().bearer_token!=canaries.canaryB)throw IOException("concurrent wrapper mutated owner B shared parameters");
        }
        // 4) Supplementary guard-registry section (patched base Request
        //    hook): the signed extension's private request path is known to
        //    bypass this registry, so this is labelled supplementary.
        {
            MockStock stockA;
            GRVHTTPRequestGuard::Register(stockA,[&stateA](BaseRequest &r,unique_ptr<HTTPClient> &c){stateA->BeforeRequest(r,c);});
            HTTPHeaders headers;
            HeadRequestInfo request(canaries.urlA,headers,*paramsA);
            auto response=stockA.Request(request);
            if(!response||response->status!=HTTPStatusCode::OK_200)throw IOException("supplementary in-scope dispatch refused");
            if(stockA.records.size()!=1||stockA.records[0].bearer!=canaries.canaryA)throw IOException("supplementary dispatch recorded the wrong canary");
            GRVHTTPRequestGuard::Remove(stockA);
        }
        // 5) Guarded SQL/catalog/settings/view/profiling canary matrix on a
        //    configured owner. Every refusal is canary-free; the only canary
        //    bytes that may ever exist are the caller's own literals.
        auto refuse=[&](void *owner,Engine &engine,const string &query,char *error,size_t capacity){
            unsigned char output[4096];size_t written=0;
            if(grv_native_metadata_query(owner,query.c_str(),output,sizeof(output),&written,error,capacity)==0) {
                throw IOException("guarded query was not refused: "+query);
            }
            if(string(error).find(canaries.canaryA)!=string::npos||string(error).find(canaries.canaryB)!=string::npos) {
                throw IOException("refusal error carried a canary: "+query);
            }
            (void)engine;
        };
        refuse(ownerA,engineA,"SELECT * FROM duckdb_secrets()",errorA,sizeof(errorA));
        refuse(ownerA,engineA,"SELECT * FROM duckdb_settings()",errorA,sizeof(errorA));
        refuse(ownerA,engineA,"CREATE VIEW leak_view AS SELECT * FROM duckdb_secrets()",errorA,sizeof(errorA));
        refuse(ownerA,engineA,"SELECT * FROM duckdb_secrets() WHERE name='_grv_exact_gcs_reader'",errorA,sizeof(errorA));
        refuse(ownerB,engineB,"SELECT * FROM duckdb_secrets()",errorB,sizeof(errorB));
        // A caller-chosen literal may enter a view; the setup canary may not.
        {
            unsigned char output[4096];size_t written=0;
            const char *create="CREATE VIEW own_view AS SELECT 'own-view-literal' AS x";
            if(grv_native_metadata_query(ownerA,create,output,sizeof(output),&written,errorA,sizeof(errorA))!=0) {
                if(string(errorA).find(canaries.canaryA)!=string::npos)throw IOException("view creation error carried a canary");
            }
        }
        // 6) Profiling: logging is disabled at setup and no request parameters
        //    carry a logger, so no log/profiling path can receive a canary.
        if(EnableLogging::GetSetting(*engineA.connection.context).GetValue<bool>()||EnableLogging::GetSetting(*engineB.connection.context).GetValue<bool>())throw IOException("logging enabled on a GCS owner");
        if(fsA.logger||fsB.logger)throw IOException("GCS request parameters retained a logger");
        if(engineA.connection.context->config.enable_profiler || engineB.connection.context->config.enable_profiler)throw IOException("profiling enabled on a GCS owner");
        // 7) Lease: the configured deadline is the admitted budget, never more;
        //    a second configure on a live owner is refused (no re-configure).
        auto now=std::chrono::steady_clock::now();
        auto budget=std::chrono::duration_cast<std::chrono::milliseconds>(stateA->deadline-now).count();
        if(budget<=0||budget>300000)throw IOException("configured deadline is not the admitted budget");
        if(grv_native_gcs_configure(ownerA,argv[1],canaries.scopeA.c_str(),canaries.canaryA.c_str(),"project-a",300000,errorA,sizeof(errorA))==0)throw IOException("second configure on a live owner was admitted");
        if(string(errorA).find(canaries.canaryA)!=string::npos)throw IOException("second-configure error carried a canary");
        // 8) Local transactions still work after setup; no canary in errors.
        {
            unsigned char output[4096];size_t written=0;
            if(grv_native_metadata_query(ownerA,"BEGIN",output,sizeof(output),&written,errorA,sizeof(errorA))!=0)throw IOException("BEGIN after setup refused: "+string(errorA));
            if(grv_native_metadata_query(ownerA,"CREATE TABLE _grv_oauth_probe(id BIGINT)",output,sizeof(output),&written,errorA,sizeof(errorA))!=0)throw IOException("local DDL after setup refused: "+string(errorA));
            if(grv_native_metadata_query(ownerA,"ROLLBACK",output,sizeof(output),&written,errorA,sizeof(errorA))!=0)throw IOException("ROLLBACK after setup refused: "+string(errorA));
        }
        // 9) Durable bytes: no canary in either owner's database files or WAL,
        //    both before close (live artifacts) and after close/flush.
        vector<string> artifacts={dbA,dbA+".wal",dbB,dbB+".wal"};
        ScanOwnedArtifacts(artifacts,canaries);
        owners.Close();
        ScanOwnedArtifacts(artifacts,canaries);
        std::printf("gcs oauth isolation proof passed\n");
        return 0;
    }catch(const std::exception &failure){
        std::fprintf(stderr,"GCS OAuth isolation proof failed: %s\n",failure.what());
    }
    // OwnerCleanup closes before WorkspaceCleanup on both success and failure.
    return 1;
}
