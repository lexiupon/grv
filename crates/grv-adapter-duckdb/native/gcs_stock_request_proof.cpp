// Signed stock HTTPFS request delegate proof, synthetic loopback credentials only.
#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "parquet_extension.hpp"
#include <chrono>
#include <iostream>
using namespace duckdb;
#include "gcs_filesystem.inc"
string Quote(const string &s){return "'"+StringUtil::Replace(s,"'","''")+"'";}
int main(int argc,char **argv){try{
    if(argc!=4)return 2;
    DuckDB db(nullptr);db.LoadStaticExtension<ParquetExtension>();Connection c(db);auto exec=[&](const string &sql){auto r=c.Query(sql);if(r->HasError())throw IOException("fixed proof setup");};
    if(string(argv[3])=="generate"){exec("COPY (SELECT 42::BIGINT AS id) TO "+Quote(argv[2])+" (FORMAT PARQUET)");return 0;}
    if(string(argv[2]).rfind("http://127.0.0.1:",0)!=0)return 2;
    exec("SET autoinstall_known_extensions=false");exec("SET autoload_known_extensions=false");
    SecretManager::Get(*c.context).SetEnablePersistentSecrets(false);exec("LOAD "+Quote(argv[1]));exec("SET httpfs_client_implementation='httplib'");exec("SET enable_external_file_cache=false");
    exec("CREATE TEMPORARY SECRET proof (TYPE http,BEARER_TOKEN 'synthetic-not-a-credential',SCOPE "+Quote(argv[2])+")");
    auto state=std::make_shared<ExactGCSState>();state->configured=true;state->active=true;state->epoch=1;state->admitted.insert(argv[2]);
    state->deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(string(argv[3])=="expired"?-1:250);uint64_t dispatch=0,bytes=0;
    if(string(argv[3])=="unlisted")state->admitted.clear();
    auto &config=DBConfig::GetConfig(*c.context);auto &stock=config.GetHTTPUtil();
    config.SetHTTPUtil(make_shared_ptr<grv::GCSHTTPPolicy>(stock,"",[&](BaseRequest &r,unique_ptr<HTTPClient> &client){
        if(r.params.timeout_usec!=0||r.params.timeout!=30)throw InternalException("request did not receive private parameters");
        state->BeforeRequest(r,client);++dispatch;
        if(r.type==RequestType::GET_REQUEST){auto &get=r.Cast<GetRequestInfo>();auto previous=get.content_handler;get.content_handler=[&,previous](const_data_ptr_t p,idx_t n){bool ok=!previous||previous(p,n);if(ok)bytes+=n;return ok;};}
    }));
    auto start=std::chrono::steady_clock::now();auto r=c.Query("SELECT id FROM read_parquet("+Quote(argv[2])+")");auto elapsed=std::chrono::steady_clock::now()-start;
    bool reject=string(argv[3])!="direct";
    bool no_request=string(argv[3])=="expired"||string(argv[3])=="unlisted";
    if(elapsed>std::chrono::seconds(3)){std::cout<<"{\"hard_deadline_failed\":true,\"elapsed_ms\":"<<std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()<<"}\n";return 3;}
    if((no_request?dispatch!=0:dispatch==0)||r->HasError()!=reject)return 3;
    if(!reject&&(bytes==0||r->GetValue(0,0).GetValue<int64_t>()!=42))return 4;
    std::cout<<"{\"dispatch\":"<<dispatch<<",\"accepted_bytes\":"<<bytes<<",\"elapsed_ms\":"<<std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()<<",\"rejected\":"<<(reject?"true":"false")<<"}\n";return 0;
}catch(...){std::cerr<<"signed request proof failed (private details suppressed)\n";return 1;}}
