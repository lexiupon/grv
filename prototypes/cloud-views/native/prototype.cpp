// Experimental only. Local immutable bytes behind a SIMULATED cloud range transport.
#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "parquet_types.h"
#include "thrift/transport/TTransport.h"
#include "thrift/protocol/TCompactProtocol.h"
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <cstring>
#include <unistd.h>

using namespace duckdb;
extern "C" int grv_native_load_static_extensions(duckdb_database, char *, size_t);

static void Require(bool ok, const std::string &message) {
    if (!ok) throw std::runtime_error(message);
}
static std::string SQL(const std::string &s) {
    std::string out = "'";
    for (char c : s) { out += c; if (c == '\'') out += c; }
    return out + "'";
}
static std::string JSON(const std::string &s) {
    std::string out = "\"";
    const char *hex = "0123456789abcdef";
    for (unsigned char c : s) {
        if (c == '"' || c == '\\') { out += '\\'; out += c; }
        else if (c < 32) { out += "\\u00"; out += hex[c >> 4]; out += hex[c & 15]; }
        else out += c;
    }
    return out + "\"";
}
struct Meter { uint64_t bytes = 0, calls = 0, rejected = 0; };
struct Object {
    const std::vector<unsigned char> bytes;
    const std::string pinned_etag = "\"fixture-etag-1\"", pinned_generation = "generation-1";
    std::string etag = pinned_etag, generation = pinned_generation;
    explicit Object(std::vector<unsigned char> b) : bytes(std::move(b)) {}
};
struct State {
    std::map<std::string, std::shared_ptr<Object>> objects;
    std::map<std::string, Meter> meters;
    std::string mutate; // etag or generation, immediately after the first successful callback
    uint64_t successes = 0;
    std::shared_ptr<Object> Exact(const std::string &uri) {
        auto it = objects.find(uri);
        if (it == objects.end()) throw PermissionException("simulated transport: exact URI not allowlisted: " + uri);
        return it->second;
    }
    void Range(const std::string &uri, uint64_t offset, uint64_t n, void *out) {
        auto o = Exact(uri); auto &m = meters[uri];
        if (o->etag != o->pinned_etag || o->generation != o->pinned_generation) {
            ++m.rejected; throw IOException("simulated transport: pinned generation/ETag mismatch");
        }
        if (!n || n > 65536 || offset > o->bytes.size() || n > o->bytes.size() - offset) {
            ++m.rejected; throw IOException("simulated transport: invalid bounded range");
        }
        // This callback stands in for a conditional 206 response. There is no network.
        std::memcpy(out, o->bytes.data() + offset, n);
        m.bytes += n; ++m.calls; ++successes;
        if (!mutate.empty() && successes == 1) {
            if (mutate == "etag") o->etag = "changed-etag";
            else o->generation = "changed-generation";
        }
    }
};
struct Handle final : FileHandle {
    idx_t position = 0;
    Handle(FileSystem &fs, std::string path, FileOpenFlags flags) : FileHandle(fs, std::move(path), flags) {}
    void Close() override {}
};
struct SimulatedFS final : FileSystem {
    std::shared_ptr<State> state;
    explicit SimulatedFS(std::shared_ptr<State> s) : state(std::move(s)) {}
    string GetName() const override { return "experimental_simulated_cloud"; }
    bool IsManuallySet() override { return true; }
    bool CanHandleFile(const string &s) override { return s.compare(0, 5, "s3://") == 0 || s.compare(0, 5, "gs://") == 0; }
    bool FileExists(const string &s, optional_ptr<FileOpener> = nullptr) override { state->Exact(s); return true; }
    bool DirectoryExists(const string &s, optional_ptr<FileOpener> = nullptr) override { state->Exact(s); return false; }
    bool IsPipe(const string &s, optional_ptr<FileOpener> = nullptr) override { state->Exact(s); return false; }
    vector<OpenFileInfo> Glob(const string &s, FileOpener * = nullptr) override {
        state->Exact(s); return {OpenFileInfo(s)}; // No expansion, decoding, prefix authority or fallback.
    }
    unique_ptr<FileHandle> OpenFile(const string &s, FileOpenFlags f, optional_ptr<FileOpener> = nullptr) override {
        state->Exact(s);
        if (!f.OpenForReading() || f.OpenForWriting() || f.CreateFileIfNotExists() || f.OverwriteExistingFile() ||
            f.OpenForAppending() || f.ExclusiveCreate() || f.Lock() == FileLockType::WRITE_LOCK)
            throw PermissionException("simulated transport: read-only");
        return make_uniq<Handle>(*this, s, f);
    }
    void Read(FileHandle &h, void *out, int64_t n, idx_t offset) override {
        auto o = state->Exact(h.path);
        if (n < 0 || offset > o->bytes.size() || uint64_t(n) > o->bytes.size() - offset)
            throw IOException("simulated transport: range outside object");
        for (uint64_t done = 0; done < uint64_t(n);) {
            auto count = std::min<uint64_t>(65536, n - done);
            state->Range(h.path, offset + done, count, static_cast<char *>(out) + done); done += count;
        }
    }
    int64_t Read(FileHandle &h, void *out, int64_t n) override {
        auto &f = h.Cast<Handle>(); auto size = state->Exact(h.path)->bytes.size();
        if (n < 0 || f.position > size) throw IOException("invalid sequential range");
        auto count = std::min<uint64_t>(n, size - f.position);
        Read(h, out, count, f.position); f.position += count; return count;
    }
    int64_t GetFileSize(FileHandle &h) override { return state->Exact(h.path)->bytes.size(); }
    FileType GetFileType(FileHandle &h) override { state->Exact(h.path); return FileType::FILE_TYPE_REGULAR; }
    timestamp_t GetLastModifiedTime(FileHandle &h) override { state->Exact(h.path); return timestamp_t(0); }
    string GetVersionTag(FileHandle &h) override { auto o = state->Exact(h.path); return o->pinned_generation + o->pinned_etag; }
    FileMetadata Stats(FileHandle &h) override { FileMetadata m; m.file_size = GetFileSize(h); m.file_type = FileType::FILE_TYPE_REGULAR; return m; }
    bool CanSeek() override { return true; }
    bool OnDiskFile(FileHandle &h) override { state->Exact(h.path); return false; }
    void Seek(FileHandle &h, idx_t n) override { state->Exact(h.path); h.Cast<Handle>().position = n; }
    idx_t SeekPosition(FileHandle &h) override { state->Exact(h.path); return h.Cast<Handle>().position; }
    void Reset(FileHandle &h) override { Seek(h, 0); }
};
// C-API database wrapper allows the exported production static-Parquet initializer;
// the query connection and filesystem use the matching patched native C++ API.
struct Engine {
    duckdb_database db = nullptr;
    std::unique_ptr<Connection> con;
    Engine() {
        Require(duckdb_open(nullptr, &db) == DuckDBSuccess, "duckdb_open failed");
        char error[2048] = {};
        if (grv_native_load_static_extensions(db, error, sizeof(error))) {
            duckdb_close(&db); throw std::runtime_error(error);
        }
        con.reset(new Connection(*reinterpret_cast<DatabaseWrapper *>(db)->database));
        Exec("SET threads=1");
        Exec("SET autoinstall_known_extensions=false"); Exec("SET autoload_known_extensions=false");
        Exec("SET enable_external_file_cache=false"); Exec("SET memory_limit='512MiB'");
    }
    ~Engine() { con.reset(); duckdb_close(&db); }
    unique_ptr<MaterializedQueryResult> Exec(const std::string &q) {
        auto r = con->Query(q); if (r->HasError()) throw std::runtime_error(r->GetError()); return r;
    }
    void Install(std::shared_ptr<State> s) {
        FileSystem::GetFileSystem(*con->context).RegisterSubSystem(make_uniq<SimulatedFS>(s));
    }
};
static std::vector<unsigned char> Load(const std::string &p) {
    std::ifstream in(p, std::ios::binary); Require(bool(in), "open fixture: " + p);
    return std::vector<unsigned char>(std::istreambuf_iterator<char>(in), {});
}
static void Save(const std::string &p, const std::vector<unsigned char> &b) {
    std::ofstream out(p, std::ios::binary); out.write(reinterpret_cast<const char *>(b.data()), b.size());
    Require(bool(out), "write fixture failed");
}
static uint32_t LE32(const unsigned char *p) { return uint32_t(p[0]) | uint32_t(p[1]) << 8 | uint32_t(p[2]) << 16 | uint32_t(p[3]) << 24; }
struct FooterBuffer final : duckdb_apache::thrift::transport::TTransport {
    std::vector<unsigned char> data;
    size_t cursor = 0;
    FooterBuffer() = default;
    FooterBuffer(const unsigned char *p, size_t n) : data(p,p+n) {}
    uint32_t read_virt(uint8_t *out, uint32_t n) override {
        auto count = std::min<size_t>(n,data.size()-cursor);
        std::memcpy(out,data.data()+cursor,count); cursor += count; return count;
    }
    void write_virt(const uint8_t *p, uint32_t n) override { data.insert(data.end(),p,p+n); }
};
// Strip only footer column statistics using DuckDB's already-linked Thrift codec.
// Data pages remain unchanged. No extra package or alternate Parquet reader.
static std::vector<unsigned char> NoStats(const std::vector<unsigned char> &b) {
    namespace tr = duckdb_apache::thrift;
    auto size = LE32(b.data() + b.size() - 8); auto start = b.size() - 8 - size;
    auto input = duckdb_base_std::make_shared<FooterBuffer>(b.data() + start, size);
    tr::protocol::TCompactProtocol reader(input); duckdb_parquet::FileMetaData meta; meta.read(&reader);
    Require(meta.row_groups.size() >= 8, "expected >=8 row groups");
    for (auto &rg : meta.row_groups) for (auto &c : rg.columns) c.meta_data.__isset.statistics = false;
    auto output = duckdb_base_std::make_shared<FooterBuffer>();
    tr::protocol::TCompactProtocol writer(output); meta.write(&writer);
    auto data = output->data.data(); uint32_t count = output->data.size();
    std::vector<unsigned char> result(b.begin(), b.begin() + start); result.insert(result.end(), data, data + count);
    for (int i = 0; i < 4; ++i) result.push_back(count >> (8 * i));
    result.insert(result.end(), {'P','A','R','1'}); return result;
}
static std::string Branch(const std::string &p, bool extra) {
    return "SELECT CAST(id AS BIGINT) AS id, CAST(amount AS DECIMAL(18,2)) AS amount, "
           "CAST(ts AS TIMESTAMP) AS ts, CAST(payload AS VARCHAR) AS payload, " +
           std::string(extra ? "CAST(extra AS VARCHAR)" : "CAST(NULL AS VARCHAR)") +
           " AS extra FROM read_parquet(" + SQL(p) + ",hive_partitioning=false,union_by_name=false)";
}
static std::string Union(const std::string &old, const std::string &fresh) { return Branch(old, false) + " UNION ALL " + Branch(fresh, true); }
static std::string Values(MaterializedQueryResult &r) {
    std::string out;
    for (auto &t : r.types) out += t.ToString() + ";";
    out += "\n";
    for (idx_t row = 0; row < r.RowCount(); ++row) {
        for (idx_t col = 0; col < r.ColumnCount(); ++col) {
            auto v = r.GetValue(col, row); out += v.IsNull() ? "NULL" : JSON(v.ToString()); out += ";";
        }
        out += "\n";
    }
    return out;
}
struct Record {
    std::string scheme, name, query, explain, result;
    std::map<std::string, Meter> meters;
    std::string cloud_sql;
    uint64_t Bytes() const { uint64_t n = 0; for (auto &m : meters) n += m.second.bytes; return n; }
    uint64_t Calls() const { uint64_t n = 0; for (auto &m : meters) n += m.second.calls; return n; }
};
struct Temp {
    std::string dir;
    std::vector<std::string> files;
    Temp() { char p[] = "/tmp/grv-pushdown-XXXXXX"; auto d = mkdtemp(p); Require(d, "mkdtemp failed"); dir = d; }
    std::string Path(const std::string &n) { auto p = dir + "/" + n; files.push_back(p); return p; }
    ~Temp() { for (auto &p : files) std::remove(p.c_str()); rmdir(dir.c_str()); }
};
static Record RunTrial(const std::string &scheme, const std::string &name, const std::string &selection,
                       const std::string &condition, const std::string &cloud, const std::string &local,
                       const std::map<std::string, std::vector<unsigned char>> &bytes) {
    auto s = std::make_shared<State>(); for (auto &p : bytes) s->objects[p.first] = std::make_shared<Object>(p.second);
    Engine e; e.Install(s);
    e.Exec("CREATE TABLE oracle AS " + local);
    e.Exec("CREATE VIEW cloud AS " + cloud);
    // Include view bind/footer traffic: it is real range callback traffic, not excluded.
    auto query = "SELECT " + selection + " FROM cloud " + condition;
    auto expected = e.Exec("SELECT " + selection + " FROM oracle " + condition);
    auto actual = e.Exec(query);
    Require(Values(*expected) == Values(*actual), scheme + " correctness: " + name);
    Record r{scheme, name, query, "", Values(*actual), s->meters, cloud};
    auto plan = e.Exec("EXPLAIN " + query);
    for (idx_t i = 0; i < plan->RowCount(); ++i) r.explain += plan->GetValue(1, i).ToString() + "\n";
    std::cout << scheme << " " << name << ": " << r.Bytes() << " bytes, " << r.Calls() << " bounded callbacks\n";
    return r;
}
#ifndef GRV_PROTOTYPE_NO_MAIN
int main(int argc, char **argv) {
    try {
        std::string output;
        for (int i = 1; i < argc; ++i) {
            if (std::string(argv[i]) == "--output" && i + 1 < argc) output = argv[++i];
            else throw std::runtime_error("usage: prototype [--output result.json]");
        }
        Require(std::string(DuckDB::LibraryVersion()) == "v1.5.6", "requires pinned DuckDB v1.5.6");
        std::cout << "Engine " << DuckDB::LibraryVersion() << " / " << DuckDB::SourceID() << "; SIMULATED transport only\n";
        Temp temp; auto old = temp.Path("old.parquet"), fresh = temp.Path("new.parquet"), empty = temp.Path("empty.parquet");
        auto nsold = temp.Path("old-no-stats.parquet"), nsnew = temp.Path("new-no-stats.parquet");
        {
            Engine e;
            std::string columns = "i::BIGINT AS id, (i%100000/100.0)::DECIMAL(18,2) AS amount, "
                "TIMESTAMP '2020-01-01' + i * INTERVAL 1 MICROSECOND AS ts, "
                "repeat(md5(i::VARCHAR),8)::VARCHAR AS payload";
            e.Exec("COPY (SELECT " + columns + " FROM range(0,32768) t(i) ORDER BY i) TO " + SQL(old) +
                   " (FORMAT PARQUET,COMPRESSION UNCOMPRESSED,ROW_GROUP_SIZE 4096,WRITE_BLOOM_FILTER false)");
            e.Exec("COPY (SELECT " + columns + ", CASE WHEN i%2=0 THEN 'present' ELSE 'absent' END::VARCHAR AS extra "
                   "FROM range(32768,65536) t(i) ORDER BY i) TO " + SQL(fresh) +
                   " (FORMAT PARQUET,COMPRESSION UNCOMPRESSED,ROW_GROUP_SIZE 4096,WRITE_BLOOM_FILTER false)");
            e.Exec("COPY (SELECT * FROM read_parquet(" + SQL(fresh) + ") WHERE false) TO " + SQL(empty) + " (FORMAT PARQUET)");
            auto meta = e.Exec("SELECT count(DISTINCT row_group_id) FROM parquet_metadata(" + SQL(old) + ")");
            Require(meta->GetValue(0,0).GetValue<int64_t>() == 8, "ordered fixture requires 8 row groups per file");
        }
        auto ob = Load(old), nb = Load(fresh), eb = Load(empty);
        Save(nsold, NoStats(ob)); Save(nsnew, NoStats(nb));
        {
            Engine e;
            auto r = e.Exec("SELECT count(*) FROM parquet_metadata(" + SQL(nsold) + ") WHERE stats_min IS NOT NULL OR stats_max IS NOT NULL");
            Require(r->GetValue(0,0).GetValue<int64_t>() == 0, "no-stats footer not stripped");
        }
        std::vector<Record> records;
        std::vector<std::string> checks;
        const std::string wide = "sum(id),sum(amount),min(ts),max(ts),sum(length(payload)),sum(length(extra))";
        for (std::string scheme : {"s3", "gs"}) {
            auto prefix = scheme + "://fixture-bucket/";
            auto a = prefix + "old.parquet", b = prefix + "new.parquet", z = prefix + "empty.parquet";
            auto special = prefix + "space%20quote'percent%25hash%23star%2A[bracket]+unicode-%E9%9B%AA.parquet";
            std::map<std::string, std::vector<unsigned char>> objects{{a,ob},{b,nb},{z,eb},{special,nb}};
            auto cloud = Union(a,b), local = Union(old,fresh);
            auto run = [&](std::string name, std::string sel, std::string cond = "") {
                auto r = RunTrial(scheme,name,sel,cond,cloud,local,objects); records.push_back(r); return r;
            };
            auto full = run("wide_full",wide);
            auto narrow = run("narrow_full","sum(id)");
            auto selective = run("selective_wide",wide,"WHERE id BETWEEN 40000 AND 40031");
            Require(narrow.Bytes() * 4 < full.Bytes(), "projection must save >75% bytes");
            Require(selective.Bytes() * 4 < full.Bytes(), "filter must save >75% bytes");
            auto evolution = run("typed_null_elimination",wide,"WHERE extra='present'");
            Require(evolution.meters[a].bytes * 10 < full.meters[a].bytes, "old typed-null branch did not eliminate data reads");
            auto noselect = run("empty_selection",wide,"WHERE id < 0");
            Require(noselect.Bytes() * 4 < full.Bytes(), "stats empty selection must avoid data");
            run("typed_values","id,amount,ts,payload,extra","WHERE id BETWEEN 32764 AND 32771 ORDER BY id");
            run("typed_filter",wide,"WHERE amount BETWEEN 400.00::DECIMAL(18,2) AND 400.31::DECIMAL(18,2) "
                "AND ts >= TIMESTAMP '2020-01-01 00:00:00.040000'");
            run("unsupported_predicate",wide,"WHERE md5(id::VARCHAR)=md5('40000')");
            auto unsuitable = run("stats_unsuitable_full_match",wide,"WHERE length(md5(id::VARCHAR))=32");
            Require(unsuitable.Bytes() * 10 >= full.Bytes() * 9, "full-match stats-unsuitable predicate unexpectedly avoided wide data");
            run("nonidentity_cast","sum(id)","WHERE CAST(id AS VARCHAR)='40000'");
            auto no_stats_objects = objects; no_stats_objects[a] = Load(nsold); no_stats_objects[b] = Load(nsnew);
            auto ns = RunTrial(scheme,"no_statistics",wide,"WHERE id BETWEEN 40000 AND 40031",cloud,Union(nsold,nsnew),no_stats_objects);
            Require(ns.Bytes() * 4 > selective.Bytes() * 5, "no-statistics should read >25% more than ordered stats"); records.push_back(ns);
            records.push_back(RunTrial(scheme,"empty_file",wide,"",Branch(z,true),Branch(empty,true),objects));
            records.push_back(RunTrial(scheme,"special_exact_uri",wide,"WHERE id=40000",Branch(special,true),Branch(fresh,true),objects));
            for (auto bad : {prefix + "unlisted.parquet", prefix + "*.parquet", prefix + "old.parquet?generation=2", prefix + "old.parquet/child"}) {
                auto s = std::make_shared<State>(); s->objects[a] = std::make_shared<Object>(ob);
                Engine e; e.Install(s); auto r = e.con->Query("SELECT * FROM read_parquet(" + SQL(bad) + ")");
                Require(r->HasError() && r->GetError().find("exact URI not allowlisted") != std::string::npos, "unlisted URI was not strictly refused");
            }
            checks.push_back(scheme + ": exact allowlist rejects unlisted/glob/query/suffix; special URI passes");
            for (std::string mutation : {"etag", "generation"}) {
                auto s = std::make_shared<State>(); s->objects[a] = std::make_shared<Object>(ob); s->mutate = mutation;
                Engine e; e.Install(s);
                auto r = e.con->Query("SELECT sum(length(payload)) FROM read_parquet(" + SQL(a) + ")");
                Require(r->HasError() && r->GetError().find("pinned generation/ETag mismatch") != std::string::npos,
                        "mutation between callbacks must refuse: " + mutation);
                Require(s->successes == 1 && s->meters[a].rejected >= 1, "cached bytes masked mutation");
                checks.push_back(scheme + ": " + mutation + " mutation refused after exactly one successful bounded callback");
                // Repeated range through same live handle cannot hide a mutation in FS cache.
                auto direct = std::make_shared<State>(); direct->objects[a] = std::make_shared<Object>(ob);
                SimulatedFS fs(direct); auto h = fs.OpenFile(a, FileFlags::FILE_FLAGS_READ); char bytes[16];
                fs.Read(*h,bytes,16,0); fs.Read(*h,bytes,16,0);
                if (mutation == "etag") direct->objects[a]->etag = "changed"; else direct->objects[a]->generation = "changed";
                bool refused = false; try { fs.Read(*h,bytes,16,0); } catch (const IOException &) { refused = true; }
                Require(refused && direct->meters[a].calls == 2, "repeat-read cache masking");
            }
        }
        if (!output.empty()) {
            std::ofstream out(output);
            out << "{\n\"experimental\":true,\"transport\":\"simulated local immutable bytes; no cloud network\",\n"
                << "\"version\":" << JSON(DuckDB::LibraryVersion()) << ",\"source_id\":" << JSON(DuckDB::SourceID())
                << ",\"row_groups_per_nonempty_file\":8,\"max_range_bytes\":65536,\"external_file_cache\":false,\"threads\":1,\n\"checks\":[";
            for (size_t i=0;i<checks.size();++i) { if(i) out << ','; out << JSON(checks[i]); }
            out << "],\n\"trials\":[\n";
            for (size_t i=0;i<records.size();++i) {
                auto &r=records[i]; if(i) out << ",\n";
                out << "{\"scheme\":" << JSON(r.scheme) << ",\"name\":" << JSON(r.name) << ",\"query\":" << JSON(r.query)
                    << ",\"cloud_view_sql\":" << JSON(r.cloud_sql)
                    << ",\"bytes\":" << r.Bytes() << ",\"calls\":" << r.Calls() << ",\"result\":" << JSON(r.result)
                    << ",\"explain\":" << JSON(r.explain) << ",\"objects\":{";
                size_t j=0; for (auto &m:r.meters) {
                    if(j++) out << ','; out << JSON(m.first) << ":{\"bytes\":" << m.second.bytes << ",\"calls\":" << m.second.calls << ",\"rejected\":" << m.second.rejected << '}';
                }
                out << "}}";
            }
            out << "\n]}\n"; Require(bool(out), "result JSON write failed");
        }
        std::cout << "PASS: " << records.size() << " correctness-oracle trials; relative-byte, allowlist, mutation checks\n";
        return 0;
    } catch (const std::exception &e) { std::cerr << "FAIL: " << e.what() << '\n'; return 1; }
}
#endif
