// Exact HTTPS authority fence. Transport and credentials remain stock HTTPFS.
#include "gcs_http_policy.hpp"
#include "duckdb/main/grv_http_request_guard.hpp"
#include <chrono>
#include <set>
namespace {
constexpr const char *GCS_HOST = "https://storage.googleapis.com/";
void GCSValidate(const string &uri, bool scope = false) {
    if (uri.size() > 8192 || !StringUtil::StartsWith(uri,GCS_HOST)) {
        throw PermissionException("invalid exact GCS HTTPS coordinate");
    }
    auto path = uri.substr(std::strlen(GCS_HOST));
    auto separator = path.find('/');
    auto bucket = path.substr(0,separator);
    if (bucket.size() < 3 || bucket.size() > 222 || bucket.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-.") != string::npos) {
        throw PermissionException("invalid GCS bucket");
    }
    for (const auto &label : StringUtil::Split(bucket,'.')) {
        if (label.empty() || label.size()>63 || label.front()=='-' || label.back()=='-') { throw PermissionException("invalid GCS bucket"); }
    }
    if (!scope && separator == string::npos) { throw PermissionException("GCS exact file requires an object key"); }
    if (separator == string::npos) { return; }
    auto key = path.substr(separator+1);
    if (key.empty()) { throw PermissionException("invalid GCS key"); }
    string decoded;
    auto hex = [](char c)->int { if(c>='0'&&c<='9') return c-'0'; if(c>='A'&&c<='F') return c-'A'+10; return -1; };
    auto unreserved = [](unsigned char c) { return (c>='a'&&c<='z') || (c>='A'&&c<='Z') || (c>='0'&&c<='9') || c=='-' || c=='.' || c=='_' || c=='~'; };
    for (size_t i=0;i<key.size();++i) {
        unsigned char c=key[i];
        if (c=='%') {
            if (i+2>=key.size() || hex(key[i+1])<0 || hex(key[i+2])<0) { throw PermissionException("invalid GCS escape"); }
            c=static_cast<unsigned char>(hex(key[i+1])*16+hex(key[i+2])); i+=2;
            if (unreserved(c) || c=='/' || c=='\\' || c<32 || c==127) { throw PermissionException("noncanonical GCS escape"); }
        } else if (c!='/' && !unreserved(c)) { throw PermissionException("noncanonical GCS key"); }
        decoded.push_back(static_cast<char>(c));
    }
    for (const auto &part : StringUtil::Split(decoded,'/')) {
        if (part.empty() || part=="." || part=="..") { throw PermissionException("invalid GCS path segment"); }
    }
}
struct ExactGCSState {
    HTTPUtil *utility=nullptr;
    ~ExactGCSState(){if(utility)GRVHTTPRequestGuard::Remove(*utility);}
    std::mutex mutex;
    bool configured=false, failed=false, active=false;
    uint64_t epoch=0;
    string scope;
#ifdef GRV_CLOUD_QUALIFICATION
    std::atomic<uint64_t> qualification_heads{0},qualification_gets{0},qualification_body_bytes{0};
#endif
    std::set<string> admitted;
    std::chrono::steady_clock::time_point deadline;
    void BeforeRequest(BaseRequest &request, unique_ptr<HTTPClient> &client) {
        Require(request.url);
        if(request.type!=RequestType::GET_REQUEST && request.type!=RequestType::HEAD_REQUEST)throw PermissionException("GCS transport is read-only");
        auto remaining=std::chrono::duration_cast<std::chrono::microseconds>(deadline-std::chrono::steady_clock::now()).count();
        if(remaining<=0)throw PermissionException("GCS request deadline expired");
        remaining=std::min<int64_t>(remaining,30000000);
        request.params.timeout=static_cast<uint64_t>(remaining/1000000);
        request.params.timeout_usec=static_cast<uint64_t>(remaining%1000000);
        request.params.retries=0;
        if(request.type==RequestType::GET_REQUEST){auto &get=request.Cast<GetRequestInfo>();auto original=get.content_handler;auto path=request.url;
            get.content_handler=[this,original,path](const_data_ptr_t data,idx_t count){Require(path);return !original||original(data,count);};}
#ifdef GRV_CLOUD_QUALIFICATION
        if(request.type==RequestType::HEAD_REQUEST){++qualification_heads;}
        else {++qualification_gets;auto &get=request.Cast<GetRequestInfo>();auto original=get.content_handler;
            get.content_handler=[this,original](const_data_ptr_t data,idx_t count){bool accepted=!original||original(data,count);if(accepted)qualification_body_bytes+=count;return accepted;};}
#endif
        // Cached stock clients were initialized with an earlier timeout. Revoke
        // them at each request boundary, then stock initializes its own client
        // from the bounded original HTTPFSParams subtype. No custom transport.
        client.reset();
    }
    void Require(const string &path, uint64_t handle_epoch=0) {
        std::lock_guard<std::mutex> guard(mutex);
        if (!configured || failed || !active || std::chrono::steady_clock::now()>=deadline ||
            !admitted.count(path) || (handle_epoch && handle_epoch!=epoch)) {
            throw PermissionException("exact GCS read authority is inactive or expired");
        }
    }
};
struct ExactGCSHandle final : FileHandle {
    std::shared_ptr<ExactGCSState> state;
    std::shared_ptr<FileSystem> stock;
    unique_ptr<FileHandle> inner;
    uint64_t epoch;
    ExactGCSHandle(FileSystem &fs, const string &path, FileOpenFlags flags,
                   std::shared_ptr<ExactGCSState> state, std::shared_ptr<FileSystem> stock,
                   unique_ptr<FileHandle> inner, uint64_t epoch)
        : FileHandle(fs,path,flags),state(std::move(state)),stock(std::move(stock)),inner(std::move(inner)),epoch(epoch) {}
    void Close() override { if(inner) { inner->Close(); inner.reset(); } }
    ~ExactGCSHandle() override { Close(); }
};
struct ExactGCSFileSystem final : FileSystem {
    std::shared_ptr<ExactGCSState> state;
    std::shared_ptr<FileSystem> stock;
    ExactGCSFileSystem(std::shared_ptr<ExactGCSState> state, unique_ptr<FileSystem> stock)
        : state(std::move(state)),stock(std::move(stock)) {}
    string GetName() const override { return "grv_exact_gcs_https"; }
    bool IsManuallySet() override { return true; }
    // Claim all stock HTTP routes so there is never a stock HTTPFS fallback.
    bool CanHandleFile(const string &path) override { return stock->CanHandleFile(path); }
    unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener=nullptr) override {
        state->Require(path);
        if (!flags.OpenForReading() || flags.OpenForWriting() || flags.CreateFileIfNotExists() ||
            flags.OverwriteExistingFile() || flags.OpenForAppending() || flags.ExclusiveCreate() ||
            flags.Lock()==FileLockType::WRITE_LOCK) { throw PermissionException("GCS opens must be read-only"); }
        uint64_t epoch;
        { std::lock_guard<std::mutex> guard(state->mutex); epoch=state->epoch; }
        auto inner=stock->OpenFile(path,flags,opener);
        state->Require(path,epoch);
        if (!inner) { throw IOException("exact GCS open failed"); }
        return make_uniq<ExactGCSHandle>(*this,path,flags,state,stock,std::move(inner),epoch);
    }
    ExactGCSHandle &Checked(FileHandle &handle) {
        auto &file=handle.Cast<ExactGCSHandle>(); file.state->Require(file.path,file.epoch);
        if (!file.inner) { throw IOException("exact GCS handle is closed"); } return file;
    }
    void Read(FileHandle &handle, void *buffer, int64_t bytes, idx_t offset) override {
        auto &file=Checked(handle); file.inner->file_system.Read(*file.inner,buffer,bytes,offset); Checked(handle);
    }
    int64_t Read(FileHandle &handle, void *buffer, int64_t bytes) override {
        auto &file=Checked(handle); auto n=file.inner->file_system.Read(*file.inner,buffer,bytes); Checked(handle); return n;
    }
    int64_t GetFileSize(FileHandle &h) override { auto &f=Checked(h); auto n=f.inner->GetFileSize(); Checked(h); return n; }
    FileType GetFileType(FileHandle &h) override { auto &f=Checked(h); return f.inner->GetType(); }
    timestamp_t GetLastModifiedTime(FileHandle &h) override { auto &f=Checked(h); auto t=f.inner->file_system.GetLastModifiedTime(*f.inner); Checked(h); return t; }
    string GetVersionTag(FileHandle &h) override { auto &f=Checked(h); auto tag=f.inner->file_system.GetVersionTag(*f.inner); Checked(h); return tag; }
    FileMetadata Stats(FileHandle &h) override { auto &f=Checked(h); auto stats=f.inner->Stats(); Checked(h); return stats; }
    void Seek(FileHandle &h, idx_t offset) override { Checked(h).inner->Seek(offset); Checked(h); }
    idx_t SeekPosition(FileHandle &h) override { return Checked(h).inner->SeekPosition(); }
    void Reset(FileHandle &h) override { Checked(h).inner->Reset(); Checked(h); }
    bool CanSeek() override { return stock->CanSeek(); }
    bool OnDiskFile(FileHandle &h) override { Checked(h); return false; }
    bool IsPipe(const string &path, optional_ptr<FileOpener> = nullptr) override { state->Require(path); return false; }
    bool FileExists(const string &path, optional_ptr<FileOpener> opener=nullptr) override { state->Require(path); auto found=stock->FileExists(path,opener); state->Require(path); return found; }
    bool DirectoryExists(const string &path, optional_ptr<FileOpener> = nullptr) override { state->Require(path); return false; }
    vector<OpenFileInfo> Glob(const string &path, FileOpener * = nullptr) override { state->Require(path); return {OpenFileInfo(path)}; }
};
}
