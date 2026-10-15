// Qualification-only counters around the actual shipping owner/scanner source.
#define GRV_CLOUD_QUALIFICATION
#include "shim.cpp"
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
struct Counters {uint64_t bytes,heads,gets;};
Counters Take(Engine &e,bool gcs){if(gcs)return {e.exact_gcs->qualification_body_bytes.exchange(0),e.exact_gcs->qualification_heads.exchange(0),e.exact_gcs->qualification_gets.exchange(0)};return {e.verified_s3->qualification_body_bytes.exchange(0),e.verified_s3->qualification_heads.exchange(0),e.verified_s3->qualification_gets.exchange(0)};}
void Emit(const string &phase,const Counters &c){std::cout<<"\""<<phase<<"\":{\"body_bytes\":"<<c.bytes<<",\"head\":"<<c.heads<<",\"get\":"<<c.gets<<"}";}
namespace {
// Closed manifest grammar for the multi-file invocation:
//   mode httpfs aws scope project-or-profile region manifest
// The twelve-argument single-file invocation above is untouched so already-qualified
// slices keep their own evidence. Only the harness is multi-file: grv_native_s3_query
// already takes a bounded file list, while the local oracle seam
// grv_native_verified_file_query admits exactly one descriptor per call (and cannot be
// opened twice over the same set), which is why the multi-file ground truth is supplied
// by the runner inside this manifest, computed independently in Python from the local
// fixtures. The guarded cloud path itself is unchanged. Nothing the runner supplies is
// echoed back verbatim: reported identity is digested, and failures stay fixed numeric
// stages. Duplicate keys, unknown fields, missing fields, empty lists and duplicate
// coordinates are refused before any remote read.
constexpr size_t MANIFEST_MAX_BYTES=1024*1024;
constexpr size_t MANIFEST_MAX_DEPTH=4;
constexpr size_t MANIFEST_MAX_FILES=4096;
constexpr size_t MANIFEST_MAX_TRIALS=10;
constexpr size_t MANIFEST_MAX_EXPECTED=1024*1024;
// Fixed numeric decision codes for the manifest path, so a failure is attributable without
// printing any string: 201 grammar, 202 bound, 203 duplicate, 204 scope, 205 oracle,
// 206 field closure. Codes are reported in the existing numeric phase slot; the single-file
// path never sets one, so its output is unchanged.
int g_manifest_code=0;
int ManifestCode(const char *stage){
    if(!std::strcmp(stage,"manifest grammar")) return 201;
    if(!std::strcmp(stage,"manifest exceeds bounded size")||!std::strcmp(stage,"manifest read")||
       !std::strcmp(stage,"manifest trailing content")||!std::strcmp(stage,"manifest root")||
       !std::strcmp(stage,"manifest files list")||!std::strcmp(stage,"manifest trials list")) return 202;
    if(!std::strcmp(stage,"manifest duplicate coordinate")||
       !std::strcmp(stage,"manifest duplicate identity")||
       !std::strcmp(stage,"manifest duplicate trial")) return 203;
    if(!std::strcmp(stage,"manifest coordinate outside scope")) return 204;
    if(!std::strcmp(stage,"oracle mismatch")) return 205;
    return 206;
}
void ManifestRequire(bool condition,const char *stage){if(!condition){g_manifest_code=ManifestCode(stage);throw InvalidInputException(stage);}}
bool LowerHex(const string &value){if(value.empty()||value.size()%2) return false;for(char c:value){if(!((c>='0'&&c<='9')||(c>='a'&&c<='f'))) return false;}return true;}
string HexDecode(const string &value){string out;for(size_t index=0;index<value.size();index+=2){auto hex=[](char c){return c<='9'?c-'0':c-'a'+10;};out+=char(hex(value[index])*16+hex(value[index+1]));}return out;}
struct ManifestValue {
    enum Kind {OBJECT,ARRAY,STRING,NUMBER} kind;
    string text;uint64_t number=0;
    vector<pair<string,ManifestValue>> members;vector<ManifestValue> items;
};
struct ManifestJson {
    const char *cursor;const char *end;
    void Fail(){g_manifest_code=201;throw InvalidInputException("manifest grammar");}
    void Skip(){while(cursor<end&&(*cursor==' '||*cursor=='\t'||*cursor=='\n'||*cursor=='\r')) ++cursor;}
    void UnicodeHex(string &out){
        unsigned code=0;for(int index=0;index<4;++index){if(cursor>=end) Fail();char c=*cursor++;int digit=c<='9'?c-'0':(c>='a'&&c<='f')?c-'a'+10:(c>='A'&&c<='F')?c-'A'+10:-1;if(digit<0) Fail();code=code*16+unsigned(digit);}
        if(code>=0xD800&&code<=0xDBFF){if(end-cursor<6||cursor[0]!='\\'||cursor[1]!='u') Fail();cursor+=2;unsigned low=0;for(int index=0;index<4;++index){char c=*cursor++;int digit=c<='9'?c-'0':(c>='a'&&c<='f')?c-'a'+10:(c>='A'&&c<='F')?c-'A'+10:-1;if(digit<0) Fail();low=low*16+unsigned(digit);}if(low<0xDC00||low>0xDFFF) Fail();code=0x10000+((code-0xD800)<<10)+(low-0xDC00);}
        else if(code>=0xD800&&code<=0xDFFF) Fail();
        if(code<0x80) out+=char(code);
        else if(code<0x800){out+=char(0xC0|(code>>6));out+=char(0x80|(code&0x3F));}
        else if(code<0x10000){out+=char(0xE0|(code>>12));out+=char(0x80|((code>>6)&0x3F));out+=char(0x80|(code&0x3F));}
        else {out+=char(0xF0|(code>>18));out+=char(0x80|((code>>12)&0x3F));out+=char(0x80|((code>>6)&0x3F));out+=char(0x80|(code&0x3F));}
    }
    void Str(string &out){
        if(cursor>=end||*cursor!='"') Fail();++cursor;out.clear();
        while(cursor<end){char c=*cursor++;
            if(c=='"') return;
            if(c=='\\'){if(cursor>=end) Fail();char e=*cursor++;
                if(e=='"') out+='"';else if(e=='\\') out+='\\';else if(e=='/') out+='/';
                else if(e=='b') out+='\b';else if(e=='f') out+='\f';else if(e=='n') out+='\n';else if(e=='r') out+='\r';else if(e=='t') out+='\t';
                else if(e=='u') UnicodeHex(out);
                else Fail();}
            else if(static_cast<unsigned char>(c)<0x20) Fail();
            else out+=c;}
        Fail();
    }
    void Value(ManifestValue &out,size_t depth){
        if(depth>MANIFEST_MAX_DEPTH) Fail();Skip();if(cursor>=end) Fail();
        if(*cursor=='{'){++cursor;out.kind=ManifestValue::OBJECT;Skip();if(cursor<end&&*cursor=='}'){++cursor;return;}
            while(true){Skip();string key;Str(key);Skip();if(cursor>=end||*cursor!=':') Fail();++cursor;
                for(auto &member:out.members){if(member.first==key) Fail();}
                out.members.emplace_back(key,ManifestValue());Value(out.members.back().second,depth+1);Skip();
                if(cursor<end&&*cursor==','){++cursor;continue;}if(cursor<end&&*cursor=='}'){++cursor;return;}Fail();}}
        if(*cursor=='['){++cursor;out.kind=ManifestValue::ARRAY;Skip();if(cursor<end&&*cursor==']'){++cursor;return;}
            while(true){out.items.emplace_back();Value(out.items.back(),depth+1);Skip();
                if(cursor<end&&*cursor==','){++cursor;continue;}if(cursor<end&&*cursor==']'){++cursor;return;}Fail();}}
        if(*cursor=='"'){out.kind=ManifestValue::STRING;Str(out.text);return;}
        if(*cursor>='0'&&*cursor<='9'){out.kind=ManifestValue::NUMBER;
            if(*cursor=='0'&&cursor+1<end&&cursor[1]>='0'&&cursor[1]<='9') Fail();
            uint64_t value=0;bool digits=false;
            while(cursor<end&&*cursor>='0'&&*cursor<='9'){digits=true;int digit=*cursor++-'0';if(value>(std::numeric_limits<uint64_t>::max()-unsigned(digit))/10) Fail();value=value*10+unsigned(digit);}
            if(!digits) Fail();out.number=value;return;}
        Fail();
    }
};
const ManifestValue &Member(const ManifestValue &value,const char *name){
    for(auto &member:value.members){if(member.first==name) return member.second;}
    throw InvalidInputException("manifest field missing");
}
void ExactMembers(const ManifestValue &value,std::initializer_list<const char *> names){
    if(value.members.size()!=names.size()) throw InvalidInputException("manifest field closure");
    for(auto &name:names){bool found=false;for(auto &member:value.members){if(member.first==name){found=true;break;}}if(!found) throw InvalidInputException("manifest field closure");}
}
struct ManifestEntry {string local,coordinate,sha256,validator;uint64_t size=0;};
struct ManifestTrial {string name,expected;};
struct ManifestResult {string name;Counters measured,planned;vector<unsigned char> actual;};
const char *const MANIFEST_TRIALS[]={"wide","narrow","selective_wide","selective_narrow","and_range","or_range","null","string","wrapper","nonidentity_cast"};
// The fixed trial SQL is the harness's own, copied from the single-file invocation; the
// manifest may only name one of these closed spellings and never supplies SQL.
string ManifestTrialSql(const string &name){
    string projection="id";for(int i=0;i<8;i++)projection+=",payload"+std::to_string(i);
    string wide="sum(id)::BIGINT";for(int i=0;i<8;i++)wide+=",sum(length(payload"+std::to_string(i)+"))::BIGINT";
    string narrow="sum(id)::BIGINT";
    if(name=="wide") return "SELECT "+wide+" FROM data";
    if(name=="narrow") return "SELECT "+narrow+" FROM data";
    if(name=="selective_wide") return "SELECT "+wide+" FROM data WHERE id<16";
    if(name=="selective_narrow") return "SELECT "+narrow+" FROM data WHERE id<16";
    if(name=="and_range") return "SELECT "+narrow+" FROM data WHERE id>=100 AND id<120";
    if(name=="or_range") return "SELECT "+narrow+" FROM data WHERE id<16 OR id>65000";
    if(name=="null") return "SELECT "+narrow+" FROM data WHERE payload0 IS NULL";
    if(name=="string") return "SELECT "+narrow+" FROM data WHERE payload0='unmatched'";
    if(name=="wrapper") return "SELECT sum(alias)::BIGINT FROM (SELECT id AS alias FROM data) q WHERE alias<16";
    if(name=="nonidentity_cast") return "SELECT sum(id)::DOUBLE FROM data WHERE CAST(id AS DOUBLE)<16";
    throw InvalidInputException("manifest trial name is not admitted");
}
string Hex(const string &bytes){static const char *digits="0123456789abcdef";string out;for(unsigned char c:bytes){out+=digits[c>>4];out+=digits[c&15];}return out;}
int ManifestMain(char **argv,void *&remote,int &fd){
    char error[4096]={0};
    ManifestRequire(string(argv[1])=="s3","manifest mode requires the protected s3 reader");
    fd=open(argv[7],O_RDONLY|O_CLOEXEC);ManifestRequire(fd>=0,"manifest");
    string raw;char buffer[65536];ssize_t read_bytes=0;
    while((read_bytes=read(fd,buffer,sizeof(buffer)))>0){ManifestRequire(raw.size()+size_t(read_bytes)<=MANIFEST_MAX_BYTES,"manifest exceeds bounded size");raw.append(buffer,size_t(read_bytes));}
    ManifestRequire(read_bytes==0,"manifest read");
    ManifestJson parser{raw.data(),raw.data()+raw.size()};
    ManifestValue root;parser.Value(root,0);parser.Skip();
    ManifestRequire(parser.cursor==parser.end,"manifest trailing content");
    ManifestRequire(root.kind==ManifestValue::OBJECT,"manifest root");
    ExactMembers(root,{"files","trials"});
    const ManifestValue &files=Member(root,"files"),&trials=Member(root,"trials");
    // Empty lists are refused: an empty manifest would silently prove nothing.
    ManifestRequire(files.kind==ManifestValue::ARRAY&&!files.items.empty()&&files.items.size()<=MANIFEST_MAX_FILES,"manifest files list");
    ManifestRequire(trials.kind==ManifestValue::ARRAY&&!trials.items.empty()&&trials.items.size()<=MANIFEST_MAX_TRIALS,"manifest trials list");
    string scope(argv[4]);vector<ManifestEntry> entries;std::set<string> coordinates,identities;
    for(auto &item:files.items){
        ManifestRequire(item.kind==ManifestValue::OBJECT,"manifest file entry");
        ExactMembers(item,{"local","coordinate","size","sha256","validator"});
        const ManifestValue &local=Member(item,"local"),&coordinate=Member(item,"coordinate"),&size=Member(item,"size"),&sha=Member(item,"sha256"),&validator=Member(item,"validator");
        ManifestRequire(local.kind==ManifestValue::STRING&&!local.text.empty()&&local.text.size()<=4096&&local.text.front()=='/'&&local.text.find_first_of("\r\n	")==string::npos,"manifest local path");
        ManifestRequire(coordinate.kind==ManifestValue::STRING&&coordinate.text.size()<=8192,"manifest coordinate");
        ManifestRequire(!scope.empty()&&coordinate.text.compare(0,scope.size(),scope)==0&&coordinate.text.size()>scope.size()&&coordinate.text[scope.size()]=='/',"manifest coordinate outside scope");
        auto parsed=S3Parse(coordinate.text);ManifestRequire(!parsed.key.empty(),"manifest coordinate key");
        ManifestRequire(size.kind==ManifestValue::NUMBER&&size.number>0&&size.number<=uint64_t(std::numeric_limits<int64_t>::max()),"manifest size");
        ManifestRequire(sha.kind==ManifestValue::STRING&&LowerHex(sha.text)&&sha.text.size()==64,"manifest sha256");
        ManifestRequire(validator.kind==ManifestValue::STRING&&!validator.text.empty()&&validator.text.size()<=1024&&validator.text.find_first_of("\r\n")==string::npos,"manifest validator");
        ManifestRequire(coordinates.insert(coordinate.text).second,"manifest duplicate coordinate");
        // Every file carries its own distinct recorded identity triple.
        ManifestRequire(identities.insert(std::to_string(size.number)+"\n"+sha.text+"\n"+validator.text).second,"manifest duplicate identity");
        entries.push_back(ManifestEntry{local.text,coordinate.text,sha.text,validator.text,size.number});
    }
    vector<ManifestTrial> listed;std::set<string> names;
    for(auto &item:trials.items){
        ManifestRequire(item.kind==ManifestValue::OBJECT,"manifest trial entry");
        ExactMembers(item,{"trial","expected"});
        const ManifestValue &name=Member(item,"trial"),&expected=Member(item,"expected");
        ManifestRequire(name.kind==ManifestValue::STRING,"manifest trial name");
        ManifestTrialSql(name.text);
        ManifestRequire(names.insert(name.text).second,"manifest duplicate trial");
        ManifestRequire(LowerHex(expected.text)&&expected.text.size()>=32&&expected.text.size()<=2*MANIFEST_MAX_EXPECTED,"manifest expected bytes");
        listed.push_back(ManifestTrial{name.text,HexDecode(expected.text)});
    }
    remote=grv_native_open(nullptr,error,sizeof(error));if(!remote)throw IOException("owner");
    auto &engine=*static_cast<Owner *>(remote)->engine;
    if(grv_native_s3_configure(remote,argv[2],argv[3],argv[4],argv[5],argv[6],error,sizeof(error)))throw IOException("setup");
    auto policy=GRVGuard::Get(*engine.connection.context);policy->relations.insert("main.data");policy->relation_catalogs["main.data"]="memory";
    vector<const char *> locations,hashes,validators;vector<uint64_t> sizes;
    for(auto &entry:entries){locations.push_back(entry.coordinate.c_str());sizes.push_back(entry.size);hashes.push_back(entry.sha256.c_str());validators.push_back(entry.validator.c_str());}
    string list;for(size_t index=0;index<locations.size();++index){if(index)list+=", ";list+=S3Literal(locations[index]);}
    string projection="id";for(int i=0;i<8;i++)projection+=",payload"+std::to_string(i);
    auto query=[&](const string &sql,int code){vector<unsigned char> output(8*1024*1024);size_t written=0;
        if(grv_native_s3_query(remote,locations.data(),sizes.data(),hashes.data(),validators.data(),locations.size(),sql.c_str(),output.data(),output.size(),&written,error,sizeof(error))||written>output.size()){g_manifest_code=code;throw IOException("qualified query");}
        output.resize(written);return output;};
    // A multi-file read_parquet requires a LIST literal; comma-separated positional
    // arguments are extra parameters and fail to bind. The list form is used uniformly so
    // the single-file case is exercised through the same construction.
    query("CREATE VIEW data AS SELECT "+projection+" FROM read_parquet(["+list+"],hive_partitioning=false,union_by_name=false)",211);
    auto bind=Take(engine,false);
    vector<ManifestResult> results;
    for(auto &trial:listed){
        // Decision point: the guarded cloud call is the only read here. The local oracle
        // seam grv_native_verified_file_query admits only ONE descriptor per call, so the
        // expected bytes for a multi-file shape cannot come from it; they are supplied by
        // the manifest, computed independently in Python from the same local fixtures.
        Take(engine,false);
        auto actual=query(ManifestTrialSql(trial.name),212);auto measured=Take(engine,false);
        if(actual.size()!=trial.expected.size()||std::memcmp(actual.data(),trial.expected.data(),actual.size())!=0){g_manifest_code=205;throw IOException("oracle mismatch");}
        auto explain=query("EXPLAIN "+ManifestTrialSql(trial.name),213);auto planned=Take(engine,false);(void)explain;
        results.push_back(ManifestResult{trial.name,measured,planned,actual});
    }
    const string first=entries.front().coordinate;
    bool direct=false,unlisted=false,glob=false,secrets=false;
    {vector<unsigned char> out(1024);size_t n=0;direct=grv_native_metadata_query(remote,("SELECT * FROM read_parquet("+S3Literal(first)+")").c_str(),out.data(),out.size(),&n,error,sizeof(error))!=0;}
    try{query("SELECT * FROM read_parquet("+S3Literal(first+"-outside")+")",0);}catch(...){unlisted=true;}
    try{query("SELECT * FROM (SELECT * FROM read_parquet("+S3Literal(first+"*")+")) q",0);}catch(...){glob=true;}
    try{query("SELECT * FROM duckdb_secrets()",0);}catch(...){secrets=true;}
    // Reported identity is digested; no provider, credential, coordinate or validator text.
    std::cout<<"{\"manifest_mode\":true,\"shipping_scanner_source\":true,\"files\":[";
    for(size_t index=0;index<entries.size();++index){if(index)std::cout<<",";std::cout<<"{\"index\":"<<index<<",\"size\":"<<entries[index].size<<",\"sha256\":\""<<entries[index].sha256<<"\",\"validator_sha256\":\""<<S3Hex(entries[index].validator)<<"\"}";}
    std::cout<<"],\"phases\":{";Emit("view_binding",bind);std::cout<<"},\"trials\":[";
    for(size_t index=0;index<results.size();++index){
        if(index)std::cout<<",";
        std::cout<<"{\"trial_index\":"<<index<<",\"trial\":\""<<results[index].name<<"\",\"correct\":true,";Emit("query",results[index].measured);std::cout<<",";Emit("explain",results[index].planned);std::cout<<",\"actual\":\""<<Hex(string(results[index].actual.begin(),results[index].actual.end()))<<"\"";
        std::cout<<"}";
    }
    std::cout<<"],\"denials\":{\"direct\":"<<(direct?"true":"false")<<",\"unlisted\":"<<(unlisted?"true":"false")<<",\"nested_glob\":"<<(glob?"true":"false")<<",\"secrets\":"<<(secrets?"true":"false")<<"}}\n";
    close(fd);grv_native_close(remote);fd=-1;
    return direct&&unlisted&&glob&&secrets&&!results.empty()?0:1;
}
}
int main(int argc,char **argv){
    // mode httpfs aws scope project-or-profile region local uri size sha validator
    // manifest invocation: mode httpfs aws scope project-or-profile region manifest
    if(argc!=12&&argc!=8){std::cerr<<"qualification arguments invalid\n";return 2;}
    void *remote=nullptr,*local=nullptr;int fd=-1;
    try {
        if(argc==8) return ManifestMain(argv,remote,fd);
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
    }catch(...){
        // Fixed numeric stages only: never expose native exception/credential/SQL
        // strings while diagnosing a failed shipping-path slice.
        int stage=0,status=0;if(remote){auto &e=*static_cast<Owner *>(remote)->engine;stage=e.verified_s3->stage;status=e.verified_s3->status;}
        if(g_manifest_code) stage=g_manifest_code;
        if(fd>=0)close(fd);if(local)grv_native_close(local);if(remote)grv_native_close(remote);
        std::cerr<<"shipping qualification failed (private details suppressed; protected read phase "<<stage<<", HTTP status "<<status<<")\n";return 1;
    }
}
