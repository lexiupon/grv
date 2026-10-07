// Exact URI authority, independent named-profile credentials, and bounded reads.
// This filesystem never delegates an S3 pathname to HTTPFS's glob/fallback path.
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "mbedtls_wrapper.hpp"
#include "s3_limits.hpp"
#include <ctime>
namespace {
using S3Crypto = duckdb_mbedtls::MbedTlsWrapper;
string S3Encode(const string &value,bool slash=false) {
    const char *digits="0123456789ABCDEF";string out;
    for(unsigned char c:value) {if((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='-'||c=='_'||c=='.'||c=='~'||(slash&&c=='/')) {out+=c;} else {out+='%';out+=digits[c>>4];out+=digits[c&15];}}
    return out;
}
// GRV uses URL path-segment encoding (including literal = and @), then
// escapes glob metacharacters. SigV4 independently uses RFC3986 encoding.
string S3GRVEncode(const string &value) {
    const char *digits="0123456789ABCDEF";string out;
    for(unsigned char c:value) {if(c=='/' || (c>=33&&c<=126&&string("\"#%<>?`{}[]*\\").find(c)==string::npos)) {out+=c;}else {out+='%';out+=digits[c>>4];out+=digits[c&15];}}
    return out;
}
struct S3Coordinate {string bucket,key;};
S3Coordinate S3Parse(const string &uri) {
    if(uri.compare(0,5,"s3://")!=0 || uri.size()>8192 || uri.find_first_of("?#\\\r\n\t")!=string::npos) {throw PermissionException("noncanonical exact S3 URI");}
    auto split=uri.find('/',5);if(split==string::npos) {throw PermissionException("exact S3 file URI requires a key");}
    S3Coordinate out{uri.substr(5,split-5),""};
    if(out.bucket.size()<3||out.bucket.size()>63||out.bucket.front()=='.'||out.bucket.back()=='.'||out.bucket.front()=='-'||out.bucket.back()=='-') {throw PermissionException("invalid S3 authority");}
    for(char c:out.bucket) {if(!((c>='a'&&c<='z')||(c>='0'&&c<='9')||c=='-'||c=='.')) {throw PermissionException("invalid S3 authority");}}
    auto encoded=uri.substr(split+1);
    auto hex=[](char c)->int {return c>='0'&&c<='9'?c-'0':c>='A'&&c<='F'?c-'A'+10:-1;};
    for(size_t index=0;index<encoded.size();++index) {unsigned char c=encoded[index];if(c=='%') {if(index+2>=encoded.size()||hex(encoded[index+1])<0||hex(encoded[index+2])<0) {throw PermissionException("invalid S3 URI escape");}c=hex(encoded[index+1])*16+hex(encoded[index+2]);index+=2;if(c=='/') {throw PermissionException("encoded S3 path separator is forbidden");}}if(c<32||c==127||c=='\\') {throw PermissionException("invalid S3 key");}out.key+=c;}
    if(S3GRVEncode(out.key)!=encoded) {throw PermissionException("S3 URI must use canonical percent encoding");}
    for(auto &part:StringUtil::Split(out.key,'/')) {if(part.empty()||part=="."||part=="..") {throw PermissionException("noncanonical S3 key segment");}}
    return out;
}
string S3Hex(const string &value) {char bytes[32],hex[64];S3Crypto::ComputeSha256Hash(value.data(),value.size(),bytes);S3Crypto::ToBase16(bytes,hex,32);return string(hex,64);}
string S3Hmac(const string &key,const string &value) {char result[32];S3Crypto::Hmac256(key.data(),key.size(),value.data(),value.size(),result);return string(result,32);}
struct S3Object {string uri,bucket,key,etag,hash,version,expected_validator;uint64_t size;};
struct VerifiedS3State {
    ClientContext *context=nullptr;
    string scope,region,key_id,secret,token;
    std::map<string,std::shared_ptr<S3Object>> objects;
    std::map<string,string> filenames;
    // Only scanner scopes expose paths; verified metadata persists for exact views.
    std::set<string> active;
    int stage=0,status=0;
    size_t metadata_bytes=0;
    bool configured=false;
    ~VerifiedS3State(){std::fill(secret.begin(),secret.end(),0);std::fill(token.begin(),token.end(),0);}
    void Check() {if(!configured||!context) {throw PermissionException("independent S3 reader is not configured");}if(context->interrupted) {throw InterruptException();}}
    HTTPHeaders Headers(const string &method,const S3Object &file,const string &path,const string &query) {
        Check();auto now=std::time(nullptr);std::tm utc;gmtime_r(&now,&utc);char date[9],stamp[17];std::strftime(date,sizeof(date),"%Y%m%d",&utc);std::strftime(stamp,sizeof(stamp),"%Y%m%dT%H%M%SZ",&utc);
        string host="s3."+region+".amazonaws.com";if(region.compare(0,3,"cn-")==0) {host+=".cn";}
        string payload="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        std::map<string,string> fields{{"host",host},{"x-amz-content-sha256",payload},{"x-amz-date",stamp}};
        if(!token.empty()) {fields["x-amz-security-token"]=token;}
        string canonical,names;HTTPHeaders headers;
        for(auto &field:fields) {canonical+=field.first+":"+field.second+"\n";if(!names.empty()) {names+=";";}names+=field.first;headers.Insert(field.first,field.second);}
        string credential=string(date)+"/"+region+"/s3/aws4_request";
        string request=method+"\n"+path+"\n"+query+"\n"+canonical+"\n"+names+"\n"+payload;
        auto signing=S3Hmac(S3Hmac(S3Hmac(S3Hmac("AWS4"+secret,date),region),"s3"),"aws4_request");
        auto signature=S3Hmac(signing,"AWS4-HMAC-SHA256\n"+string(stamp)+"\n"+credential+"\n"+S3Hex(request));char hex[64];S3Crypto::ToBase16(&signature[0],hex,32);
        headers.Insert("authorization","AWS4-HMAC-SHA256 Credential="+key_id+"/"+credential+", SignedHeaders="+names+", Signature="+string(hex,64));
        if(!file.etag.empty()) {headers.Insert("if-match",file.etag);}return headers;
    }
    unique_ptr<HTTPResponse> Request(S3Object &file,bool head,uint64_t start,uint64_t bytes,void *output) {
        Check();stage=head?20:30;status=0;auto &util=HTTPUtil::Get(*context->db);
        string host="s3."+region+".amazonaws.com"+(region.compare(0,3,"cn-")==0?".cn":"");
        string base="https://"+host,path="/"+file.bucket+"/"+S3Encode(file.key,true),query=file.version.empty()?"":"versionId="+S3Encode(file.version);
        string url=base+path+(query.empty()?"":"?"+query);
        auto params=util.InitializeParameters(*context,url);
        params->follow_location=false;params->override_verify_ssl=true;params->verify_ssl=true;params->timeout=10;params->timeout_usec=0;params->retries=0;params->keep_alive=false;params->extra_headers.clear();params->http_proxy.clear();params->http_proxy_username.clear();params->http_proxy_password.clear();params->logger.reset();
        auto client=util.InitializeClient(*params,base);stage=head?21:31;auto headers=Headers(head?"HEAD":"GET",file,path,query);
        unique_ptr<HTTPResponse> response;
        if(head) {HeadRequestInfo info(url,headers,*params);response=client->Head(info);status=response?int(response->status):0;stage=22;}
        else {
            if(bytes==0||bytes>65536||start>file.size||bytes>file.size-start) {throw IOException("S3 bounded read is outside verified object");}
            headers.Insert("range","bytes="+std::to_string(start)+"-"+std::to_string(start+bytes-1));uint64_t done=0;bool accepted=false;
            GetRequestInfo info(url,headers,*params,[&](const HTTPResponse &value) {
                status=int(value.status);stage=32;accepted=int(value.status)==206 && value.GetHeaderValue("etag")==file.etag && value.GetHeaderValue("content-length")==std::to_string(bytes) && value.GetHeaderValue("content-range")=="bytes "+std::to_string(start)+"-"+std::to_string(start+bytes-1)+"/"+std::to_string(file.size) && (file.version.empty()||value.GetHeaderValue("x-amz-version-id")==file.version);
                return accepted;
            },[&](const_data_ptr_t data,idx_t count) {if(!accepted||context->interrupted||count>bytes-done) {return false;}std::memcpy(static_cast<char *>(output)+done,data,count);done+=count;return true;});
            response=client->Get(info);if(!accepted||done!=bytes) {throw IOException("S3 conditional bounded read refused");}
        }
        // Do not return clients to an extension-wide cache or retain per-file buffers.
        client->Cleanup();client.reset();Check();
        if(!response||!response->Success()||int(response->status)!=(head?200:206)) {throw IOException("S3 exact read failed");}
        return response;
    }
    std::shared_ptr<S3Object> Verify(const string &uri,uint64_t size,const string &hash,const string &etag) {
        Check();stage=10;status=0;auto coordinate=S3Parse(uri);
        if(uri.compare(0,scope.size(),scope)!=0||uri.size()<=scope.size()||uri[scope.size()]!='/') {throw PermissionException("S3 file is outside configured complete scope");}
        if(hash.size()!=64||etag.empty()||etag.size()>1024||etag.find_first_of("\r\n")!=string::npos||size>uint64_t(std::numeric_limits<int64_t>::max())) {throw PermissionException("invalid exact S3 file identity");}
        auto prior=objects.find(uri);if(prior!=objects.end()) {auto &p=*prior->second;if(p.size!=size||p.hash!=hash||p.expected_validator!=etag) {throw PermissionException("S3 URI identity changed during fixed lifecycle");}return prior->second;}
        size_t retained=0;
        if(!grv_s3_limits::Admit(metadata_bytes,objects.size(),uri.size(),etag.size(),retained)) {throw PermissionException("S3 exact file metadata exceeds bounded allowance");}
        auto file=std::make_shared<S3Object>(S3Object{uri,coordinate.bucket,coordinate.key,etag,hash,"",etag,size});
        file->etag.clear();auto response=Request(*file,true,0,0,nullptr);
        stage=23;const auto &actual_etag=response->GetHeaderValue("etag");if(actual_etag.empty()||actual_etag.size()>grv_s3_limits::MAX_VALIDATOR_BYTES||actual_etag.find_first_of("\r\n")!=string::npos||response->GetHeaderValue("content-length")!=std::to_string(size)) {throw IOException("S3 object differs from parent-verified identity");}
        file->etag=response->GetHeaderValue("etag");
        stage=24;file->version=response->HasHeader("x-amz-version-id")?response->GetHeaderValue("x-amz-version-id"):string();if(file->version=="null") {file->version.clear();}if(file->version.size()>grv_s3_limits::MAX_VERSION_BYTES) {throw IOException("S3 object version identity exceeds bound");}
        stage=25;if(file->etag!=etag) {
            S3Crypto::SHA256State sha;unsigned char buffer[65536];for(uint64_t cursor=0;cursor<size;) {auto count=std::min<uint64_t>(sizeof(buffer),size-cursor);Request(*file,false,cursor,count,buffer);sha.AddBytes(buffer,count);cursor+=count;}
            stage=40;char actual[64];sha.FinishHex(actual);if(string(actual,64)!=hash) {throw IOException("S3 object failed full SHA256 verification");}
        }
        const string filename="s3://"+file->bucket+"/"+file->key;
        auto alias=filenames.find(filename);if(alias!=filenames.end()&&alias->second!=uri) {throw PermissionException("decoded S3 filename has ambiguous canonical authority");}
        filenames.emplace(filename,uri);objects.emplace(uri,file);metadata_bytes+=retained;return file;
    }
    std::shared_ptr<S3Object> Require(const string &filename) {Check();auto alias=filenames.find(filename);if(alias==filenames.end()) {throw PermissionException("S3 scanner requires its exact decoded filename");}auto match=objects.find(alias->second);if(match==objects.end()||!active.count(alias->second)) {throw PermissionException("S3 scanner rejects unverified or unauthorized exact URI");}return match->second;}
};
struct VerifiedS3Handle final:FileHandle {std::shared_ptr<S3Object> object;idx_t position=0;VerifiedS3Handle(FileSystem &fs,string path,FileOpenFlags flags,std::shared_ptr<S3Object> object):FileHandle(fs,std::move(path),flags),object(std::move(object)){}void Close() override {} };
struct VerifiedS3FileSystem final:FileSystem {
    std::shared_ptr<VerifiedS3State> state;explicit VerifiedS3FileSystem(std::shared_ptr<VerifiedS3State> state):state(std::move(state)){}
    string GetName() const override {return "grv_verified_s3";}bool IsManuallySet() override {return true;}bool CanHandleFile(const string &path) override {return path.compare(0,5,"s3://")==0;}
    bool FileExists(const string &path,optional_ptr<FileOpener> =nullptr) override {state->Require(path);return true;}
    bool DirectoryExists(const string &path,optional_ptr<FileOpener> =nullptr) override {state->Require(path);return false;}
    bool IsPipe(const string &path,optional_ptr<FileOpener> =nullptr) override {state->Require(path);return false;}
    vector<OpenFileInfo> Glob(const string &path,FileOpener * =nullptr) override {state->Require(path);return {OpenFileInfo(path)};}
    unique_ptr<FileHandle> OpenFile(const string &path,FileOpenFlags flags,optional_ptr<FileOpener> =nullptr) override {
        auto file=state->Require(path);if(!flags.OpenForReading()||flags.OpenForWriting()||flags.CreateFileIfNotExists()||flags.OverwriteExistingFile()||flags.OpenForAppending()||flags.ExclusiveCreate()||flags.Lock()==FileLockType::WRITE_LOCK) {throw PermissionException("S3 exact scanner permits read-only opens");}return make_uniq<VerifiedS3Handle>(*this,path,flags,file);
    }
    VerifiedS3Handle &Checked(FileHandle &handle) {state->Require(handle.path);return handle.Cast<VerifiedS3Handle>();}
    void Read(FileHandle &handle,void *buffer,int64_t bytes,idx_t offset) override {
        auto &file=Checked(handle);if(bytes<0||offset>file.object->size||uint64_t(bytes)>file.object->size-offset) {throw IOException("S3 range outside exact object");}
        for(uint64_t done=0;done<uint64_t(bytes);) {auto count=std::min<uint64_t>(65536,uint64_t(bytes)-done);state->Request(*file.object,false,offset+done,count,static_cast<char *>(buffer)+done);done+=count;}
    }
    int64_t Read(FileHandle &handle,void *buffer,int64_t bytes) override {auto &file=Checked(handle);if(bytes<0||file.position>file.object->size) {throw IOException("invalid S3 sequential range");}auto count=std::min<uint64_t>(bytes,file.object->size-file.position);Read(handle,buffer,count,file.position);file.position+=count;return count;}
    int64_t GetFileSize(FileHandle &handle) override {return Checked(handle).object->size;}
    FileType GetFileType(FileHandle &handle) override {Checked(handle);return FileType::FILE_TYPE_REGULAR;}
    timestamp_t GetLastModifiedTime(FileHandle &handle) override {Checked(handle);return timestamp_t(0);}
    string GetVersionTag(FileHandle &handle) override {return Checked(handle).object->hash;}
    FileMetadata Stats(FileHandle &handle) override {FileMetadata result;result.file_size=GetFileSize(handle);result.file_type=FileType::FILE_TYPE_REGULAR;return result;}
    bool CanSeek() override {return true;}bool OnDiskFile(FileHandle &handle) override {Checked(handle);return false;}
    void Seek(FileHandle &handle,idx_t offset) override {Checked(handle).position=offset;}idx_t SeekPosition(FileHandle &handle) override {return Checked(handle).position;}void Reset(FileHandle &handle) override {Seek(handle,0);}
};
}
