// Qualification-only counters around the actual shipping owner/scanner source.
#define GRV_CLOUD_QUALIFICATION
#include "shim.cpp"
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
struct Counters {uint64_t bytes,heads,gets;};
Counters Take(Engine &e,bool gcs){if(gcs)return {e.exact_gcs->qualification_body_bytes.exchange(0),e.exact_gcs->qualification_heads.exchange(0),e.exact_gcs->qualification_gets.exchange(0)};return {e.verified_s3->qualification_body_bytes.exchange(0),e.verified_s3->qualification_heads.exchange(0),e.verified_s3->qualification_gets.exchange(0)};}
void Emit(const string &phase,const Counters &c){std::cout<<"\""<<phase<<"\":{\"body_bytes\":"<<c.bytes<<",\"head\":"<<c.heads<<",\"get\":"<<c.gets<<"}";}
int main(int argc,char **argv){
    // mode httpfs aws scope project-or-profile region local uri size sha validator
    if(argc!=12){std::cerr<<"qualification arguments invalid\n";return 2;}
    void *remote=nullptr,*local=nullptr;int fd=-1;
    try {
        bool gcs=string(argv[1])=="gcs";if(!gcs&&string(argv[1])!="s3")throw InvalidInputException("mode");
        char error[4096]={0};remote=grv_native_open(nullptr,error,sizeof(error));local=grv_native_open(nullptr,error,sizeof(error));if(!remote||!local)throw IOException("owner");
        auto &engine=*static_cast<Owner *>(remote)->engine;int status;
        if(gcs){GCSPrivateString bearer("");std::getline(std::cin,bearer.value);if(bearer.value.empty()||bearer.value.size()>16384)throw PermissionException("credential input");status=grv_native_gcs_configure(remote,argv[2],argv[4],bearer.value.c_str(),argv[5],300000,error,sizeof(error));}
        else status=grv_native_s3_configure(remote,argv[2],argv[3],argv[4],argv[5],argv[6],error,sizeof(error));
        if(status)throw IOException("setup");fd=open(argv[7],O_RDONLY|O_CLOEXEC);if(fd<0)throw IOException("fixture");
        string canonical=argv[8],filename;
        if(gcs){if(!StringUtil::StartsWith(canonical,"gs://"))throw IOException("uri");auto coordinate=S3Parse("s3://"+canonical.substr(5));filename=string(GCS_HOST)+coordinate.bucket+"/"+S3Encode(coordinate.key,true);}
        else {auto coordinate=S3Parse(canonical);filename="s3://"+coordinate.bucket+"/"+coordinate.key;}
        const char *locations[]={gcs?filename.c_str():canonical.c_str()};uint64_t sizes[]={std::stoull(argv[9])};const char *hashes[]={argv[10]},*validators[]={argv[11]};
        auto query=[&](void *owner,const string &sql,bool cloud){vector<unsigned char> output(8*1024*1024);size_t written=0;int code;
            if(!cloud)code=grv_native_verified_file_query(owner,fd,sql.c_str(),output.data(),output.size(),&written,error,sizeof(error));
            else if(gcs)code=grv_native_gcs_query(owner,locations,1,sql.c_str(),output.data(),output.size(),&written,error,sizeof(error));
            else code=grv_native_s3_query(owner,locations,sizes,hashes,validators,1,sql.c_str(),output.data(),output.size(),&written,error,sizeof(error));
            if(code||written>output.size())throw IOException("qualified query");output.resize(written);return output;};
        string projection="id";for(int i=0;i<8;i++)projection+=",payload"+std::to_string(i);
        query(local,"CREATE VIEW data AS SELECT "+projection+" FROM read_parquet('/dev/fd/"+std::to_string(fd)+"',hive_partitioning=false,union_by_name=false)",false);
        Take(engine,gcs);query(remote,"CREATE VIEW data AS SELECT "+projection+" FROM read_parquet("+S3Literal(filename)+",hive_partitioning=false,union_by_name=false)",true);auto bind=Take(engine,gcs);
        for(auto *owner:{remote,local}){auto policy=GRVGuard::Get(*static_cast<Owner *>(owner)->engine->connection.context);policy->relations.insert("main.data");policy->relation_catalogs["main.data"]="memory";}
        string wide="sum(id)::BIGINT";for(int i=0;i<8;i++)wide+=",sum(length(payload"+std::to_string(i)+"))::BIGINT";
        vector<std::pair<string,string>> cases={{"wide","SELECT "+wide+" FROM data"},{"narrow","SELECT sum(id)::BIGINT FROM data"},{"selective_wide","SELECT "+wide+" FROM data WHERE id<16"},{"selective_narrow","SELECT sum(id)::BIGINT FROM data WHERE id<16"},{"and_range","SELECT sum(id)::BIGINT FROM data WHERE id>=100 AND id<120"},{"or_range","SELECT sum(id)::BIGINT FROM data WHERE id<16 OR id>65000"},{"null","SELECT sum(id)::BIGINT FROM data WHERE payload0 IS NULL"},{"string","SELECT sum(id)::BIGINT FROM data WHERE payload0='unmatched'"},{"wrapper","SELECT sum(alias)::BIGINT FROM (SELECT id AS alias FROM data) q WHERE alias<16"},{"nonidentity_cast","SELECT sum(id)::DOUBLE FROM data WHERE CAST(id AS DOUBLE)<16"}};
        std::cout<<"{\"provider\":\""<<(gcs?"gcs":"protected_s3")<<"\",\"shipping_scanner_source\":true,\"phases\":{";Emit("view_binding",bind);std::cout<<"},\"trials\":[";bool first=true;uint64_t baseline=0,narrow=0,selective=0;
        for(auto &trial:cases){auto expected=query(local,trial.second,false);Take(engine,gcs);auto actual=query(remote,trial.second,true);auto measured=Take(engine,gcs);if(actual!=expected)throw IOException("oracle mismatch");auto explain=query(remote,"EXPLAIN "+trial.second,true);auto planned=Take(engine,gcs);(void)explain;
            if(trial.first=="wide")baseline=measured.bytes;if(trial.first=="narrow")narrow=measured.bytes;if(trial.first=="selective_wide")selective=measured.bytes;
            if(!first)std::cout<<",";first=false;std::cout<<"{\"trial\":\""<<trial.first<<"\",\"correct\":true,";Emit("query",measured);std::cout<<",";Emit("explain",planned);std::cout<<"}";
        }
        bool direct=false,unlisted=false,glob=false,secrets=false;
        {vector<unsigned char> out(1024);size_t n=0;direct=grv_native_metadata_query(remote,("SELECT * FROM read_parquet("+S3Literal(filename)+")").c_str(),out.data(),out.size(),&n,error,sizeof(error))!=0;}
        try{query(remote,"SELECT * FROM read_parquet("+S3Literal(filename+"-outside")+")",true);}catch(...){unlisted=true;}
        try{query(remote,"SELECT * FROM (SELECT * FROM read_parquet("+S3Literal(filename+"*")+")) q",true);}catch(...){glob=true;}
        try{query(remote,"SELECT * FROM duckdb_secrets()",true);}catch(...){secrets=true;}
        std::cout<<"],\"denials\":{\"direct\":"<<(direct?"true":"false")<<",\"unlisted\":"<<(unlisted?"true":"false")<<",\"nested_glob\":"<<(glob?"true":"false")<<",\"secrets\":"<<(secrets?"true":"false")<<"},\"qualifying_savings\":{\"projection\":"<<(baseline?1.0-double(narrow)/baseline:0)<<",\"selective_wide\":"<<(baseline?1.0-double(selective)/baseline:0)<<"}}\n";
        close(fd);grv_native_close(local);grv_native_close(remote);return direct&&unlisted&&glob&&secrets&&baseline>0?0:1;
    }catch(...){if(fd>=0)close(fd);if(local)grv_native_close(local);if(remote)grv_native_close(remote);std::cerr<<"shipping qualification failed (private details suppressed)\n";return 1;}
}
