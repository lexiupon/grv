// Standalone stock HTTPFS feasibility harness; NOT a production filesystem guard.
#include "duckdb.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
// Exact pinned stock extension layout; no extension methods/symbols are called.
#include "http_state.hpp"
#include <fstream>
#include <iostream>
#include <cstdlib>
#include <sstream>
#include <vector>
using namespace duckdb;
static std::string SetupStage = "engine";
extern "C" int grv_native_load_static_extensions(duckdb_database,char *,size_t);
extern "C" void *grv_native_open(const char *,char *,size_t);
extern "C" int grv_native_begin(void *,const char *,char *,size_t);
extern "C" void grv_native_close(void *);
static std::string Q(const std::string &s) { std::string o="'"; for(char c:s) {o+=c;if(c=='\'')o+=c;}return o+"'"; }
static std::string J(const std::string &s) { std::string o="\""; for(unsigned char c:s) {if(c=='"'||c=='\\') {o+='\\';o+=c;}else if(c=='\n')o+="\\n";else if(c>=32)o+=c;} return o+"\""; }
static std::string Env(const char *k) {auto p=getenv(k);if(!p||!*p)throw std::runtime_error("missing explicit HMAC/metadata input");return p;}
struct Obj {std::string uri,local;bool extra;};
struct HTTPMetrics {
    idx_t head=0,get=0,put=0,post=0,del=0,received=0,sent=0;
    std::string JSON() const {
        return "{\"requests\":"+std::to_string(head+get+put+post+del)+
            ",\"head\":"+std::to_string(head)+",\"get\":"+std::to_string(get)+
            ",\"put\":"+std::to_string(put)+",\"post\":"+std::to_string(post)+
            ",\"delete\":"+std::to_string(del)+",\"bytes_received\":"+std::to_string(received)+
            ",\"bytes_sent\":"+std::to_string(sent)+"}";
    }
};
struct Engine {
    duckdb_database db=nullptr;std::unique_ptr<Connection> c;
    Engine(const std::string &extension) {
        duckdb_config config;duckdb_create_config(&config);
        for(auto k:{"autoinstall_known_extensions","autoload_known_extensions","allow_unsigned_extensions"}) duckdb_set_config(config,k,"false");
        duckdb_set_config(config,"threads","1");
        char *err=nullptr;auto status=duckdb_open_ext(nullptr,&db,config,&err);duckdb_destroy_config(&config);duckdb_free(err);
        if(status!=DuckDBSuccess)throw std::runtime_error("engine setup failed");
        char error[2048]={};if(grv_native_load_static_extensions(db,error,sizeof(error)))throw std::runtime_error("static Parquet load failed");
        c.reset(new Connection(*reinterpret_cast<DatabaseWrapper *>(db)->database));
        SecretManager::Get(*c->context).SetEnablePersistentSecrets(false);
        SetupStage="load";Exec("LOAD "+Q(extension));SetupStage="settings";Exec("SET enable_external_file_cache=false");Exec("SET memory_limit='512MiB'");
        // Explicit empty defaults prevent inherited S3 settings from being used.
        for(auto k:{"s3_access_key_id","s3_secret_access_key","s3_session_token"})Exec(std::string("SET ")+k+"=''");
        Exec("SET http_retries=0");Exec("SET s3_url_compatibility_mode=true");
    }
    ~Engine(){c.reset();duckdb_close(&db);}
    unique_ptr<MaterializedQueryResult> Exec(const std::string &sql){auto r=c->Query(sql);if(r->HasError())throw std::runtime_error("native query failed (details suppressed)");return r;}
    HTTPMetrics HTTP() {
        auto state=c->context->registered_state->Get<HTTPState>("http_state");
        if(!state) return {}; // Before the first remote bind there may be no HTTP state.
        return {state->head_count.load(),state->get_count.load(),state->put_count.load(),
                state->post_count.load(),state->delete_count.load(),
                state->total_bytes_received.load(),state->total_bytes_sent.load()};
    }
    unique_ptr<MaterializedQueryResult> Measured(const std::string &sql,HTTPMetrics &metrics) {
        // Materialized Query() calls QueryEnd before returning (HTTPState resets there).
        // PendingQuery binds and executes fully, but does not end the query until Execute().
        // Snapshot after RESULT_READY, before result fetch/QueryEnd or any later EXPLAIN.
        auto pending=c->PendingQuery(sql,false);
        if(pending->HasError())throw std::runtime_error("native bind failed (details suppressed)");
        PendingExecutionResult status;
        do {status=pending->ExecuteTask();} while(!PendingQueryResult::IsResultReady(status));
        if(pending->HasError() || status==PendingExecutionResult::EXECUTION_ERROR)
            throw std::runtime_error("native execution failed (details suppressed)");
        metrics=HTTP();
        auto result=pending->Execute();
        if(result->HasError() || result->type!=QueryResultType::MATERIALIZED_RESULT)
            throw std::runtime_error("native result failed (details suppressed)");
        return unique_ptr_cast<QueryResult,MaterializedQueryResult>(std::move(result));
    }
    bool Reject(const std::string &sql){return c->Query(sql)->HasError();}
    void Secret(const std::vector<Obj> &objs,const std::string &endpoint,bool ssl,bool payer){
        std::string scope="[";for(auto &o:objs){if(scope.size()>1)scope+=",";scope+=Q(o.uri);}scope+="]";
        if(Env("GRV_HTTPFS_AUTH")=="oauth") {
            std::string headers;
            if(payer)headers=", EXTRA_HTTP_HEADERS MAP {'x-goog-user-project': "+Q(Env("GRV_HTTPFS_PROJECT"))+"}";
            // BEARER_TOKEN is natively redacted. Never place it in extra headers.
            Exec("CREATE TEMPORARY SECRET prototype_hmac (TYPE http, PROVIDER config, BEARER_TOKEN "+Q(Env("GRV_HTTPFS_BEARER_TOKEN"))+", SCOPE "+scope+headers+")");
        } else {
            Exec("CREATE TEMPORARY SECRET prototype_hmac (TYPE gcs, PROVIDER config, KEY_ID "+Q(Env("GRV_HTTPFS_HMAC_ID"))+", SECRET "+Q(Env("GRV_HTTPFS_HMAC_SECRET"))+", SCOPE "+scope+", ENDPOINT "+Q(endpoint)+", URL_STYLE 'path', USE_SSL "+(ssl?"true":"false")+", URL_COMPATIBILITY_MODE true, REGION 'us-east-1', REQUESTER_PAYS "+(payer?"true":"false")+")");
        }
    }
    void Fence(const std::vector<Obj> &objs){std::string paths="[";for(auto&o:objs){if(paths.size()>1)paths+=",";paths+=Q(o.uri);}paths+="]";Exec("SET allowed_paths="+paths);Exec("SET enable_external_access=false");}
};
static std::string Branch(const Obj&o,bool remote){return "SELECT CAST(id AS BIGINT) AS id, CAST(amount AS DECIMAL(18,2)) AS amount, CAST(ts AS TIMESTAMP) AS ts, CAST(payload AS VARCHAR) AS payload, "+std::string(o.extra?"CAST(extra AS VARCHAR)":"CAST(NULL AS VARCHAR)")+" AS extra FROM read_parquet("+Q(remote?o.uri:o.local)+",hive_partitioning=false,union_by_name=false)";}
static std::string Union(const std::vector<Obj>&v,bool remote){std::string q;for(auto&o:v){if(!q.empty())q+=" UNION ALL ";q+=Branch(o,remote);}return q;}
static std::string Values(MaterializedQueryResult&r){std::string o;for(auto&t:r.types)o+=t.ToString()+";";for(idx_t i=0;i<r.RowCount();++i)for(idx_t j=0;j<r.ColumnCount();++j){auto v=r.GetValue(j,i);o+=v.IsNull()?"<NULL>":v.ToString();o+='\n';}return o;}
static void Replace(std::string&s,const std::string&from,const std::string&to){size_t p=0;while((p=s.find(from,p))!=std::string::npos){s.replace(p,from.size(),to);p+=to.size();}}
int main(int argc,char**argv){
    std::string stage="inputs";
    try {
        if(argc!=7)throw std::runtime_error("usage mismatch");
        std::string manifest=argv[1],extension=argv[2],out=argv[3],mode=argv[4],endpoint=argv[5],trial=argv[6];
        if(std::string(DuckDB::LibraryVersion())!="v1.5.6")throw std::runtime_error("wrong native engine");
        Env("GRV_HTTPFS_ACCOUNT");Env("GRV_HTTPFS_PROJECT");
        bool live=mode=="LIVE";if(!live && mode!="TEST")throw std::runtime_error("invalid mode");
        if((live && endpoint!="storage.googleapis.com")||(!live && endpoint.rfind("127.0.0.1:",0)!=0))throw std::runtime_error("endpoint policy");
        std::ifstream input(manifest);if(!input)throw std::runtime_error("manifest open failed");std::vector<Obj>objs;std::string line;
        while(std::getline(input,line)){std::istringstream row(line);Obj o;std::string extra;std::getline(row,o.uri,'\t');std::getline(row,o.local,'\t');std::getline(row,extra);o.extra=extra=="1";objs.push_back(o);}
        if(objs.empty())throw std::runtime_error("empty manifest");
        stage="engine";Engine e(extension);stage="oracle";e.Exec("CREATE TABLE oracle AS "+Union(objs,false));
        stage="secret";e.Secret(objs,endpoint,live,trial=="payer");stage="fence";e.Fence(objs);
        std::ofstream report(out);if(!report)throw std::runtime_error("report open failed");report<<"{\"trial\":"<<J(trial)<<",\"engine\":\"v1.5.6\",\"mode\":"<<J(mode)<<",\"authentication\":"<<J(Env("GRV_HTTPFS_AUTH"));
        if(trial=="guard"){
            stage="owner";char err[2048]={};void*owner=grv_native_open(nullptr,err,sizeof(err));if(!owner)throw std::runtime_error("owner setup");
            std::string direct="SELECT id::BIGINT FROM read_parquet("+Q(objs[0].uri)+")";
            bool rejected=grv_native_begin(owner,direct.c_str(),err,sizeof(err))!=0;
            bool nested=grv_native_begin(owner,("SELECT * FROM ("+direct+") q").c_str(),err,sizeof(err))!=0;
            bool glob=grv_native_begin(owner,("SELECT id::BIGINT FROM read_parquet("+Q(objs[0].uri+"*")+")").c_str(),err,sizeof(err))!=0;
            grv_native_close(owner);
            report<<",\"production_owner_direct_rejected\":"<<rejected<<",\"production_owner_nested_rejected\":"<<nested<<",\"production_owner_glob_rejected\":"<<glob;
            // Exploratory write probe only: failure is not proof of a read-only fence.
            stage="write";Engine writer(extension);
            writer.Exec("SET allowed_paths=["+Q(out)+"]");writer.Exec("SET enable_external_access=false");
            ClientContextFileOpener opener(*writer.c->context);
            bool write_ok=false;
            try {auto h=FileSystem::GetFileSystem(*writer.c->context).OpenFile(out,FileFlags::FILE_FLAGS_WRITE, &opener);h->Close();write_ok=true;} catch(...) {}
            report<<",\"allowlisted_write_open_succeeded\":"<<write_ok;
        } else {
            HTTPMetrics view_http,query_http,explain_http;
            stage="view";e.Measured("CREATE VIEW cloud AS "+Union(objs,true),view_http);
            std::string selection="id,amount,ts,payload,extra",cond="ORDER BY id";
            if(trial=="narrow")selection="id,extra";
            if(trial=="selective"){selection="id,extra";cond="WHERE id >= 4096 AND id < 4112 ORDER BY id";}
            if(trial=="selective-wide" || trial=="no-stats-selective")cond="WHERE id >= 4096 AND id < 4112 ORDER BY id";
            if(trial=="null-elimination")cond="WHERE extra='present' ORDER BY id";
            stage="correctness";auto expected=e.Exec("SELECT "+selection+" FROM oracle "+cond);auto actual=e.Measured("SELECT "+selection+" FROM cloud "+cond,query_http);
            if(Values(*expected)!=Values(*actual))throw std::runtime_error("oracle mismatch");
            stage="plan";auto plan=e.Measured("EXPLAIN SELECT "+selection+" FROM cloud "+cond,explain_http);std::string p;for(idx_t i=0;i<plan->RowCount();++i)p+=plan->GetValue(1,i).ToString();
            for(size_t i=0;i<objs.size();++i){Replace(p,objs[i].uri,"<object-"+std::to_string(i)+">");Replace(p,objs[i].local,"<oracle-"+std::to_string(i)+">");}
            report<<",\"correct\":true,\"rows\":"<<actual->RowCount()<<",\"plan\":"<<J(p)
                  <<",\"native_http\":{\"source\":\"stock_httpfs_http_state\","
                    "\"byte_semantics\":\"HTTP body bytes, not wire/header/TLS bytes\","
                    "\"capture\":\"pending result ready, before fetch and QueryEnd reset\","
                    "\"view_binding\":"<<view_http.JSON()<<",\"query\":"<<query_http.JSON()
                  <<",\"explain\":"<<explain_http.JSON()<<"}";
            stage="denials";
            report<<",\"nonallowlisted_rejected\":"<<e.Reject("SELECT * FROM read_parquet("+Q(objs[0].uri+"-other")+")")
                  <<",\"glob_rejected\":"<<e.Reject("SELECT * FROM read_parquet("+Q(objs[0].uri+"*")+")")
                  <<",\"nested_nonallowlisted_rejected\":"<<e.Reject("SELECT * FROM (SELECT * FROM read_parquet("+Q(objs[0].uri+"-other")+")) q");
            // Scope is a prefix, not exact authority. Do not send an out-of-scope cloud request.
            e.Exec("DROP SECRET prototype_hmac");
        }
        report<<"}\n";return 0;
    } catch(...) {std::cerr<<"prototype failed at "<<(stage=="engine"?SetupStage:stage)<<" (SQL, credentials, error details suppressed)\n";return 1;}
}
