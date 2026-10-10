// Experimental opt-in live GCS transport. No credential discovery or cloud mutation.
#define GRV_PROTOTYPE_NO_MAIN
#include "prototype.cpp"
#include "duckdb/common/crypto/md5.hpp"
#include <curl/curl.h>
#include <array>
#include <chrono>
#include <filesystem>
#include <set>
#include <sstream>
#include <fcntl.h>

namespace live {
namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
constexpr uint64_t MAX_BYTES = 120000000, MAX_CALLS = 2500;
static bool Decimal(const std::string &s) {
    return !s.empty() && s.size() <= 20 && s[0] != '0' && s.find_first_not_of("0123456789") == std::string::npos;
}
static uint64_t Number(const std::string &s) {
    Require(Decimal(s), "expected positive decimal string");
    size_t n = 0; auto v = std::stoull(s, &n); Require(n == s.size(), "invalid integer"); return v;
}
static std::string Encode(const std::string &s, bool slash = false) {
    const char *hex = "0123456789ABCDEF"; std::string out;
    for (unsigned char c : s) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
            c == '-' || c == '_' || c == '.' || c == '~' || (slash && c == '/')) out += c;
        else { out += '%'; out += hex[c >> 4]; out += hex[c & 15]; }
    }
    return out;
}
static std::string Endpoint(const std::string &uri, const std::string &generation) {
    Require(uri.compare(0, 5, "gs://") == 0, "only canonical gs URI allowed");
    auto split = uri.find('/', 5); Require(split != std::string::npos, "missing object key");
    auto bucket = uri.substr(5, split - 5), path = uri.substr(split + 1);
    Require(bucket.size() >= 3 && bucket.size() <= 222 &&
            bucket.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789.-_") == std::string::npos,
            "invalid bucket");
    std::string key;
    for (size_t i = 0; i < path.size(); ++i) {
        unsigned char c = path[i];
        if (c == '%') {
            Require(i + 2 < path.size(), "invalid URI escape");
            auto hex = [](char h) -> int { if (h >= '0' && h <= '9') return h-'0'; if (h >= 'A' && h <= 'F') return h-'A'+10; return -1; };
            int a = hex(path[i+1]), b = hex(path[i+2]); Require(a >= 0 && b >= 0, "uppercase URI escapes required");
            c = (a << 4) | b; i += 2;
        }
        Require(c != 0 && c != '\r' && c != '\n', "control bytes in key forbidden"); key += c;
    }
    Require(!key.empty() && key.size() <= 1024 && Encode(key, true) == path, "noncanonical object URI");
    // Decode the canonical URI exactly once, then encode the entire object-name path segment.
    return "https://storage.googleapis.com/download/storage/v1/b/" + Encode(bucket) + "/o/" + Encode(key) +
           "?alt=media&generation=" + generation + "&ifGenerationMatch=" + generation;
}
struct Entry { std::string role, uri, local, generation, endpoint; uint64_t size; };
static std::vector<Entry> Manifest(const std::string &path) {
    Require(fs::is_regular_file(path) && fs::file_size(path) <= 65536, "manifest exceeds 64KiB");
    std::ifstream in(path); Require(bool(in), "manifest open failed"); std::vector<Entry> entries;
    std::set<std::string> roles, uris; std::string line;
    while (std::getline(in, line)) {
        Require(line.size() <= 8192 && entries.size() < 6, "manifest size limit");
        std::vector<std::string> fields; size_t start = 0;
        for (;;) { auto p = line.find('\t', start); fields.push_back(line.substr(start, p-start)); if (p == std::string::npos) break; start = p+1; }
        Require(fields.size() == 5, "manifest needs five tab-delimited fields, no header");
        auto &role = fields[0]; Require(role == "old" || role == "new" || role == "empty" || role == "old-no-stats" || role == "new-no-stats" || role == "special", "invalid manifest role");
        Require(roles.insert(role).second && uris.insert(fields[1]).second, "duplicate role or URI");
        auto size = Number(fields[3]); Require(size <= 20000000, "fixture exceeds 20MB"); Number(fields[4]);
        Require(fs::path(fields[2]).is_absolute() && fs::is_regular_file(fields[2]) && fs::file_size(fields[2]) == size, "local fixture path/size mismatch");
        entries.push_back({role,fields[1],fields[2],fields[4],Endpoint(fields[1],fields[4]),size});
    }
    Require(in.eof(), "manifest read failure");
    for (auto role : {"old","new","empty","old-no-stats","new-no-stats"}) Require(roles.count(role), "missing required role");
    return entries;
}
struct Budget {
    uint64_t requests = 0, reserved = 0, received = 0, accepted_bytes = 0, accepted_calls = 0;
    Clock::time_point end = Clock::now() + std::chrono::minutes(10);
    void Check() const { Require(Clock::now() < end, "live deadline exceeded"); }
};
struct Response {
    std::array<unsigned char,65536> bytes{};
    size_t used = 0, limit = 0, header_bytes = 0;
    std::string range, length, encoding, generation;
    bool bad = false;
    std::set<std::string> seen;
    Budget *budget;
};
static size_t Body(char *data, size_t size, size_t count, void *arg) noexcept {
    auto &r = *static_cast<Response *>(arg); auto n = size * count;
    r.budget->received += n;
    if (n > r.limit-r.used || r.budget->received > MAX_BYTES || Clock::now() >= r.budget->end) { r.bad = true; return 0; }
    std::memcpy(r.bytes.data()+r.used,data,n); r.used += n; return n;
}
static size_t Header(char *data, size_t size, size_t count, void *arg) noexcept {
    auto &r = *static_cast<Response *>(arg); auto n = size * count;
    try {
        r.header_bytes += n;
        if (r.header_bytes > 16384) { r.bad = true; return 0; }
        std::string line(data,n);
        if (line.compare(0,5,"HTTP/") == 0) return n;
        auto p = line.find(':'); if (p == std::string::npos) return n;
        auto key = line.substr(0,p); for (auto &c : key) c = std::tolower(static_cast<unsigned char>(c));
        auto value = line.substr(p+1); auto first = value.find_first_not_of(" \t\r\n"), last = value.find_last_not_of(" \t\r\n");
        value = first == std::string::npos ? "" : value.substr(first,last-first+1);
        if (key == "content-range" || key == "content-length" || key == "content-encoding" || key == "x-goog-generation") {
            if (!r.seen.insert(key).second) { r.bad = true; return 0; }
            if (key == "content-range") r.range = value;
            if (key == "content-length") r.length = value;
            if (key == "content-encoding") r.encoding = value;
            if (key == "x-goog-generation") r.generation = value;
        }
        return n;
    } catch (...) { r.bad = true; return 0; }
}
static int Progress(void *arg, curl_off_t, curl_off_t, curl_off_t, curl_off_t) {
    return Clock::now() >= static_cast<Budget *>(arg)->end;
}
struct Transport {
    std::map<std::string,Entry> objects;
    std::map<std::string,Meter> meters;
    std::shared_ptr<Budget> budget;
    std::string token, quota;
    Transport(const std::vector<Entry> &entries, std::shared_ptr<Budget> b, std::string t, std::string q)
        : budget(std::move(b)), token(std::move(t)), quota(std::move(q)) { for (auto &e : entries) objects.emplace(e.uri,e); }
    const Entry &Exact(const std::string &uri) {
        auto p = objects.find(uri); if (p == objects.end()) throw PermissionException("live GCS exact allowlist refusal"); return p->second;
    }
    void Range(const std::string &uri, uint64_t offset, uint64_t n, void *out) {
        auto &e = Exact(uri); auto &m = meters[uri]; budget->Check();
        Require(n && n <= 65536 && offset <= e.size && n <= e.size-offset, "invalid bounded range");
        Require(budget->requests < MAX_CALLS && n <= MAX_BYTES-budget->reserved, "live request/byte budget exceeded");
        ++budget->requests; budget->reserved += n;
        CURL *raw = curl_easy_init(); Require(raw, "curl initialization failed");
        std::unique_ptr<CURL,decltype(&curl_easy_cleanup)> curl(raw,curl_easy_cleanup);
        curl_slist *headers = nullptr;
        auto add = [&](const std::string &s) { auto p = curl_slist_append(headers,s.c_str()); Require(p, "header allocation failed"); headers = p; };
        add("Authorization: Bearer " + token); add("x-goog-user-project: " + quota); add("Accept-Encoding: identity");
        std::unique_ptr<curl_slist,decltype(&curl_slist_free_all)> owned(headers,curl_slist_free_all);
        Response response; response.limit = n; response.budget = budget.get();
        auto range = std::to_string(offset) + "-" + std::to_string(offset+n-1);
        auto set = [&](CURLoption option, auto value) { Require(curl_easy_setopt(raw,option,value) == CURLE_OK,"curl option failed"); };
        set(CURLOPT_URL,e.endpoint.c_str()); set(CURLOPT_HTTPHEADER,headers); set(CURLOPT_RANGE,range.c_str());
        set(CURLOPT_PROXY,""); set(CURLOPT_NOPROXY,"*"); set(CURLOPT_FOLLOWLOCATION,0L); set(CURLOPT_MAXREDIRS,0L);
        set(CURLOPT_PROTOCOLS,long(CURLPROTO_HTTPS)); set(CURLOPT_REDIR_PROTOCOLS,long(CURLPROTO_HTTPS));
        set(CURLOPT_SSL_VERIFYPEER,1L); set(CURLOPT_SSL_VERIFYHOST,2L); set(CURLOPT_NETRC,long(CURL_NETRC_IGNORED));
        set(CURLOPT_HTTP_CONTENT_DECODING,0L); set(CURLOPT_VERBOSE,0L); set(CURLOPT_NOSIGNAL,1L);
        set(CURLOPT_CONNECTTIMEOUT_MS,10000L);
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(budget->end-Clock::now()).count();
        set(CURLOPT_TIMEOUT_MS,long(std::min<int64_t>(30000,std::max<int64_t>(1,remaining))));
        set(CURLOPT_WRITEFUNCTION,&Body); set(CURLOPT_WRITEDATA,&response);
        set(CURLOPT_HEADERFUNCTION,&Header); set(CURLOPT_HEADERDATA,&response);
        set(CURLOPT_NOPROGRESS,0L); set(CURLOPT_XFERINFOFUNCTION,&Progress); set(CURLOPT_XFERINFODATA,budget.get());
        auto code = curl_easy_perform(raw); long status = 0; curl_easy_getinfo(raw,CURLINFO_RESPONSE_CODE,&status);
        auto expected = "bytes " + range + "/" + std::to_string(e.size);
        if (code != CURLE_OK || response.bad || status != 206 || response.range != expected ||
            response.length != std::to_string(n) || response.used != n ||
            (!response.encoding.empty() && response.encoding != "identity") ||
            (!response.generation.empty() && response.generation != e.generation)) {
            ++m.rejected; throw IOException("live GCS rejected response (status=" + std::to_string(status) + ", curl=" + std::to_string(int(code)) + ")");
        }
        budget->Check(); std::memcpy(out,response.bytes.data(),n);
        m.bytes += n; ++m.calls; budget->accepted_bytes += n; ++budget->accepted_calls;
    }
};
struct GCSFS final : FileSystem {
    std::shared_ptr<Transport> state;
    explicit GCSFS(std::shared_ptr<Transport> s) : state(std::move(s)) {}
    string GetName() const override { return "experimental_live_gcs"; }
    bool IsManuallySet() override { return true; }
    bool CanHandleFile(const string &s) override { return s.compare(0,5,"gs://") == 0 || s.compare(0,5,"s3://") == 0; }
    bool FileExists(const string &s, optional_ptr<FileOpener> = nullptr) override { state->Exact(s); return true; }
    bool DirectoryExists(const string &s, optional_ptr<FileOpener> = nullptr) override { state->Exact(s); return false; }
    bool IsPipe(const string &s, optional_ptr<FileOpener> = nullptr) override { state->Exact(s); return false; }
    vector<OpenFileInfo> Glob(const string &s, FileOpener * = nullptr) override { state->Exact(s); return {OpenFileInfo(s)}; }
    unique_ptr<FileHandle> OpenFile(const string &s, FileOpenFlags f, optional_ptr<FileOpener> = nullptr) override {
        state->Exact(s);
        if (!f.OpenForReading() || f.OpenForWriting() || f.CreateFileIfNotExists() || f.OverwriteExistingFile() ||
            f.OpenForAppending() || f.ExclusiveCreate() || f.Lock() == FileLockType::WRITE_LOCK) throw PermissionException("live GCS read-only");
        return make_uniq<Handle>(*this,s,f);
    }
    void Read(FileHandle &h, void *out, int64_t n, idx_t offset) override {
        auto size = state->Exact(h.path).size;
        if (n < 0 || offset > size || uint64_t(n) > size-offset) throw IOException("live GCS range outside object");
        for (uint64_t done = 0; done < uint64_t(n);) {
            auto count = std::min<uint64_t>(65536,n-done); state->Range(h.path,offset+done,count,static_cast<char *>(out)+done); done += count;
        }
    }
    int64_t Read(FileHandle &h, void *out, int64_t n) override {
        auto &f = h.Cast<Handle>(); auto size = state->Exact(h.path).size;
        if (n < 0 || f.position > size) throw IOException("invalid sequential range");
        auto count = std::min<uint64_t>(n,size-f.position); Read(h,out,count,f.position); f.position += count; return count;
    }
    int64_t GetFileSize(FileHandle &h) override { return state->Exact(h.path).size; }
    FileType GetFileType(FileHandle &h) override { state->Exact(h.path); return FileType::FILE_TYPE_REGULAR; }
    timestamp_t GetLastModifiedTime(FileHandle &h) override { state->Exact(h.path); return timestamp_t(0); }
    string GetVersionTag(FileHandle &h) override { return state->Exact(h.path).generation; }
    FileMetadata Stats(FileHandle &h) override { FileMetadata m; m.file_size = GetFileSize(h); m.file_type = FileType::FILE_TYPE_REGULAR; return m; }
    bool CanSeek() override { return true; }
    bool OnDiskFile(FileHandle &h) override { state->Exact(h.path); return false; }
    void Seek(FileHandle &h, idx_t n) override { Require(n <= state->Exact(h.path).size,"seek outside object"); h.Cast<Handle>().position = n; }
    idx_t SeekPosition(FileHandle &h) override { state->Exact(h.path); return h.Cast<Handle>().position; }
    void Reset(FileHandle &h) override { Seek(h,0); }
};
static std::string HashFile(const std::string &path) {
    MD5Context hash; std::ifstream in(path,std::ios::binary); std::array<char,65536> buffer;
    while (in) { in.read(buffer.data(),buffer.size()); hash.Add(reinterpret_cast<const_data_ptr_t>(buffer.data()),in.gcount()); }
    Require(in.eof(),"hash read failure"); return hash.FinishHex();
}
static void Export(const std::string &dir) {
    Require(fs::path(dir).is_absolute() && fs::create_directory(dir),"export requires explicitly given fresh absolute directory");
    auto old = dir+"/old.parquet", fresh = dir+"/new.parquet", empty = dir+"/empty.parquet";
    Engine e;
    std::string columns = "i::BIGINT AS id, (i%100000/100.0)::DECIMAL(18,2) AS amount, "
        "TIMESTAMP '2020-01-01' + i * INTERVAL 1 MICROSECOND AS ts, repeat(md5(i::VARCHAR),8)::VARCHAR AS payload";
    std::string options = " (FORMAT PARQUET,COMPRESSION UNCOMPRESSED,ROW_GROUP_SIZE 4096,WRITE_BLOOM_FILTER false)";
    e.Exec("COPY (SELECT "+columns+" FROM range(0,32768) t(i) ORDER BY i) TO "+SQL(old)+options);
    e.Exec("COPY (SELECT "+columns+", CASE WHEN i%2=0 THEN 'present' ELSE 'absent' END::VARCHAR AS extra "
           "FROM range(32768,65536) t(i) ORDER BY i) TO "+SQL(fresh)+options);
    e.Exec("COPY (SELECT * FROM read_parquet("+SQL(fresh)+") WHERE false) TO "+SQL(empty)+" (FORMAT PARQUET)");
    Save(dir+"/old-no-stats.parquet",NoStats(Load(old))); Save(dir+"/new-no-stats.parquet",NoStats(Load(fresh)));
    std::ofstream out(dir+"/fixtures.json"); out << "{\"experimental\":true,\"hash_algorithm\":\"md5 (byte identity only)\",\"fixtures\":[";
    bool comma = false;
    for (auto role : {"old","new","empty","old-no-stats","new-no-stats"}) {
        auto path = dir+"/"+role+".parquet"; auto size = fs::file_size(path); auto hash = HashFile(path);
        if (comma) out << ','; comma = true;
        out << "{\"role\":" << JSON(role) << ",\"path\":" << JSON(path) << ",\"size\":" << JSON(std::to_string(size)) << ",\"md5\":" << JSON(hash) << '}';
        std::cout << role << '\t' << path << '\t' << size << '\t' << hash << '\n';
    }
    out << "]}\n"; Require(bool(out),"fixture metadata write failure");
}
static std::string Redact(std::string text, const std::vector<Entry> &entries, const std::string &token) {
    auto replace = [&](const std::string &from, const std::string &to) {
        if (from.empty()) return; size_t p = 0;
        while ((p = text.find(from,p)) != std::string::npos) { text.replace(p,from.size(),to); p += to.size(); }
    };
    replace(token,"[TOKEN]");
    for (auto &e : entries) { replace(SQL(e.uri),SQL("gs://redacted/"+e.role)); replace(e.uri,"gs://redacted/"+e.role); replace(e.local,"[local:"+e.role+"]"); }
    return text;
}
static void Scan(const std::string &manifest, const std::string &quota, const std::string &output) {
    Require(quota == "entit-finance-prod", "explicit authorized quota project required");
    auto entries = Manifest(manifest);
    const char *env = std::getenv("GRV_PROTOTYPE_GCS_TOKEN");
    Require(env && std::strlen(env) >= 10 && std::strlen(env) <= 16384,"GRV_PROTOTYPE_GCS_TOKEN required");
    std::string token(env); Require(token.find_first_of("\r\n") == std::string::npos,"invalid token");
    Require(curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK,"curl global init failed");
    struct Cleanup { ~Cleanup() { curl_global_cleanup(); } } cleanup;
    int fd = open(output.c_str(),O_CREAT|O_EXCL|O_WRONLY,0600); Require(fd >= 0,"output must be fresh writable file"); close(fd);
    std::ofstream out(output); Require(bool(out),"output open failed");
    auto budget = std::make_shared<Budget>();
    std::map<std::string,Entry> roles; for (auto &e : entries) roles.emplace(e.role,e);
    auto cloud = Union(roles.at("old").uri,roles.at("new").uri);
    auto local = Union(roles.at("old").local,roles.at("new").local);
    out << "{\"experimental\":true,\"transport\":\"live GCS HTTPS conditional ranges\",\"version\":" << JSON(DuckDB::LibraryVersion())
        << ",\"source_id\":" << JSON(DuckDB::SourceID()) << ",\"max_range_bytes\":65536,\"max_payload_bytes\":120000000,\"max_requests\":2500,"
        << "\"deadline_seconds\":600,\"memory_limit\":\"512MiB per fresh engine\",\"external_file_cache\":false,\"threads\":1,\"trials\":[\n";
    bool comma = false; std::map<std::string,Record> records; std::string failure;
    try {
        auto run = [&](const std::string &name, const std::string &selection, const std::string &condition,
                       const std::string &remote, const std::string &oracle) {
            budget->Check(); auto transport = std::make_shared<Transport>(entries,budget,token,quota);
            Engine e; e.Exec("SET max_temp_directory_size='0B'");
            FileSystem::GetFileSystem(*e.con->context).RegisterSubSystem(make_uniq<GCSFS>(transport));
            e.Exec("CREATE TABLE oracle AS "+oracle); e.Exec("CREATE VIEW cloud AS "+remote);
            auto query = "SELECT "+selection+" FROM cloud "+condition;
            auto expected = e.Exec("SELECT "+selection+" FROM oracle "+condition); auto actual = e.Exec(query);
            Require(Values(*expected) == Values(*actual),"correctness oracle mismatch: "+name);
            auto plan = e.Exec("EXPLAIN "+query); std::string explain;
            for (idx_t i = 0; i < plan->RowCount(); ++i) explain += plan->GetValue(1,i).ToString()+"\n";
            Record r{"gs",name,query,explain,Values(*actual),transport->meters,remote};
            if (comma) out << ",\n"; comma = true;
            out << "{\"name\":" << JSON(name) << ",\"correct\":true,\"query\":" << JSON(Redact(query,entries,token))
                << ",\"cloud_view_sql\":" << JSON(Redact(remote,entries,token)) << ",\"bytes\":" << r.Bytes() << ",\"calls\":" << r.Calls()
                << ",\"result\":" << JSON(r.result) << ",\"explain\":" << JSON(Redact(explain,entries,token)) << ",\"objects\":{";
            bool objcomma = false; for (auto &entry : entries) {
                auto m = transport->meters[entry.uri]; if (objcomma) out << ','; objcomma = true;
                out << JSON(entry.role) << ":{\"bytes\":" << m.bytes << ",\"calls\":" << m.calls << ",\"rejected\":" << m.rejected << '}';
            }
            out << "}}"; out.flush(); Require(bool(out),"result write failed"); records[name] = r;
            std::cout << name << ": " << r.Bytes() << " accepted range bytes, " << r.Calls() << " accepted calls; oracle PASS\n";
        };
        const std::string wide = "sum(id),sum(amount),min(ts),max(ts),sum(length(payload)),sum(length(extra))";
        run("wide_full",wide,"",cloud,local);
        run("narrow_full","sum(id)","",cloud,local);
        run("selective_wide",wide,"WHERE id BETWEEN 40000 AND 40031",cloud,local);
        run("typed_null_elimination",wide,"WHERE extra='present'",cloud,local);
        run("decimal_timestamp_values","id,amount,ts,payload,extra","WHERE id BETWEEN 32764 AND 32771 ORDER BY id",cloud,local);
        run("decimal_timestamp_filter",wide,"WHERE amount BETWEEN 400.00::DECIMAL(18,2) AND 400.31::DECIMAL(18,2) AND ts >= TIMESTAMP '2020-01-01 00:00:00.040000'",cloud,local);
        run("empty_selection",wide,"WHERE id < 0",cloud,local);
        run("empty_file",wide,"",Branch(roles.at("empty").uri,true),Branch(roles.at("empty").local,true));
        run("no_statistics",wide,"WHERE id BETWEEN 40000 AND 40031",Union(roles.at("old-no-stats").uri,roles.at("new-no-stats").uri),Union(roles.at("old-no-stats").local,roles.at("new-no-stats").local));
        if (roles.count("special")) run("special_exact_uri",wide,"WHERE id=40000",Branch(roles.at("special").uri,true),Branch(roles.at("special").local,true));
        auto &full = records.at("wide_full");
        Require(records.at("narrow_full").Bytes()*4 < full.Bytes(),"projection reduction regression");
        Require(records.at("selective_wide").Bytes()*4 < full.Bytes(),"selectivity reduction regression");
        auto olduri = roles.at("old").uri;
        Require(records.at("typed_null_elimination").meters[olduri].bytes*10 < full.meters[olduri].bytes,"typed-null branch reduction regression");
        Require(records.at("no_statistics").Bytes()*4 > records.at("selective_wide").Bytes()*5,"no-statistics reduction regression");
        budget->Check();
    } catch (const std::exception &) {
        // DuckDB/curl errors may embed object paths or server text. Never retain them.
        failure = "live scan failed: transport, budget, bind, correctness or reduction check; inspect completed redacted trials";
    }
    out << "\n],\"success\":" << (failure.empty()?"true":"false") << ",\"failure\":" << JSON(failure)
        << ",\"accepted_range_bytes\":" << budget->accepted_bytes << ",\"accepted_range_calls\":" << budget->accepted_calls
        << ",\"attempted_requests\":" << budget->requests << ",\"reserved_payload_bytes\":" << budget->reserved
        << ",\"received_payload_bytes\":" << budget->received
        << ",\"special_key_status\":" << JSON(roles.count("special")?"trial requested; see success/trials":"not tested: optional special role absent")
        << ",\"gaps\":[\"No live generation mutation test; all requests pinned by generation and ifGenerationMatch\",\"No redirects, proxies, URI aliases, glob expansion or credential discovery\",\"Key NUL/CR/LF unsupported; percent-encoded slash aliases rejected\"]}\n";
    Require(bool(out),"result write failed"); Require(failure.empty(),failure);
}
static void SelfTest() {
    const std::string uri = "gs://fixture-bucket/space%20quote%27percent%25hash%23star%2A%5Bbracket%5D%2Bunicode-%E9%9B%AA.parquet";
    auto endpoint = Endpoint(uri,"123456789");
    Require(endpoint == "https://storage.googleapis.com/download/storage/v1/b/fixture-bucket/o/space%20quote%27percent%25hash%23star%2A%5Bbracket%5D%2Bunicode-%E9%9B%AA.parquet?alt=media&generation=123456789&ifGenerationMatch=123456789", "special conversion mismatch");
    Require(Endpoint("gs://fixture-bucket/a/b%252F.parquet","1").find("/o/a%2Fb%252F.parquet?") != std::string::npos,"single decode mismatch");
    for (auto bad : {"gs://fixture-bucket/a%2Fb", "gs://fixture-bucket/a+", "gs://fixture-bucket/a#", "gs://fixture-bucket/a%2a", "gs://fixture-bucket/a%00", "gs://fixture-bucket/a?generation=1"}) {
        bool refused = false; try { Endpoint(bad,"1"); } catch (...) { refused = true; }
        Require(refused,"noncanonical URI accepted");
    }
    auto budget = std::make_shared<Budget>();
    Entry entry{"old","gs://fixture-bucket/old.parquet","/unused","1","unused",100};
    auto transport = std::make_shared<Transport>(std::vector<Entry>{entry},budget,"not-a-real-token","entit-finance-prod");
    GCSFS filesystem(transport);
    auto h = filesystem.OpenFile(entry.uri,FileFlags::FILE_FLAGS_READ);
    filesystem.Seek(*h,100); Require(filesystem.SeekPosition(*h) == 100 && filesystem.GetFileSize(*h) == 100,"seek/size mismatch");
    for (auto path : {"gs://fixture-bucket/*.parquet", "gs://fixture-bucket/old.parquet?generation=1", "gs://fixture-bucket/old.parquet/child", "s3://fixture-bucket/old.parquet"}) {
        bool refused = false; try { filesystem.Glob(path); } catch (...) { refused = true; }
        Require(refused,"allowlist mismatch");
    }
    bool refused = false; try { filesystem.OpenFile(entry.uri,FileFlags::FILE_FLAGS_WRITE); } catch (...) { refused = true; }
    Require(refused && budget->requests == 0,"write refusal or offline isolation failed");
    Response response; response.limit = 3; response.budget = budget.get();
    char body[] = "abcd";
    Require(Body(body,1,4,&response) == 0 && response.bad && response.used == 0,"oversized body accepted");
    Response headers; headers.budget = budget.get();
    std::string line = "Content-Range: bytes 0-2/100\r\n";
    Require(Header(line.data(),1,line.size(),&headers) == line.size() && headers.range == "bytes 0-2/100","range parsing failed");
    Require(Header(line.data(),1,line.size(),&headers) == 0 && headers.bad,"duplicate header accepted");
    std::cout << "PASS: live offline URI conversion, exact allowlist, read-only, seek, bounded body/header checks; zero network requests\n";
}
} // namespace live
int main(int argc, char **argv) {
    try {
        Require(std::string(DuckDB::LibraryVersion()) == "v1.5.6","requires pinned DuckDB v1.5.6");
        Require(argc >= 2,"usage: live export --directory ABS_FRESH_DIR | scan --manifest ABS_TSV --quota-project entit-finance-prod --output FRESH_JSON");
        std::string mode = argv[1]; std::map<std::string,std::string> args;
        for (int i = 2; i < argc; i += 2) { Require(i+1 < argc,"missing argument value"); Require(args.emplace(argv[i],argv[i+1]).second,"duplicate argument"); }
        if (mode == "export") { Require(args.size() == 1 && args.count("--directory"),"export requires --directory only"); live::Export(args.at("--directory")); }
        else if (mode == "self-test") { Require(args.empty(),"self-test takes no arguments"); live::SelfTest(); }
        else if (mode == "scan") {
            Require(args.size() == 3 && args.count("--manifest") && args.count("--quota-project") && args.count("--output"),"scan arguments required");
            live::Scan(args.at("--manifest"),args.at("--quota-project"),args.at("--output"));
        } else throw std::runtime_error("unknown mode");
        return 0;
    } catch (const std::exception &) { std::cerr << "FAIL: live prototype; validate arguments/fixtures or inspect redacted output. No server error text retained.\n"; return 1; }
}
