#include "grv_guard.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/grv_scalar_profile.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/parser.hpp"
#include "parquet_extension.hpp"
#include <cstring>
#include <memory>
#include <mutex>
#include <limits>

using namespace duckdb;
#include "verified_fd.inc"
#include "verified_s3.inc"
namespace {
struct AcquiredTable {
    vector<LogicalType> types;
    idx_t rows = 0, cursor = 0;
    string name;
    bool complete = false;
};
struct Engine {
    DuckDB database;
    Connection connection;
    unique_ptr<QueryResult> result;
    vector<AcquiredTable> stages;
    bool staged = false;
    bool snapshot_started = false, acquisition_sealed = false, multi_acquisition = false;
    std::shared_ptr<VerifiedS3State> verified_s3 = std::make_shared<VerifiedS3State>();
    std::shared_ptr<VerifiedDescriptorState> verified_descriptors = std::make_shared<VerifiedDescriptorState>();
    Engine(const char *path, DBConfig &configuration) : database(path, &configuration), connection(database) {
        verified_s3->context=connection.context.get();
        FileSystem::GetFileSystem(*connection.context).RegisterSubSystem(make_uniq<VerifiedS3FileSystem>(verified_s3));
        FileSystem::GetFileSystem(*connection.context).RegisterSubSystem(make_uniq<VerifiedDescriptorFileSystem>(verified_descriptors));
    }
};
struct GRVInterruptControl { std::mutex mutex; Engine *engine = nullptr; };
struct Owner { std::unique_ptr<Engine> engine; std::shared_ptr<GRVInterruptControl> interrupt; };
struct InterruptOwner { std::shared_ptr<GRVInterruptControl> state; };
void Error(char *buffer, size_t capacity, const std::exception &error) {
    if (capacity) { std::strncpy(buffer, error.what(), capacity-1); buffer[capacity-1] = '\0'; }
}
shared_ptr<GRVGuard> Policy(ClientContext &context) {
    auto policy = make_shared_ptr<GRVGuard>();
    auto &catalog = Catalog::GetSystemCatalog(context);
    catalog.ScanSchemas(context,[&](SchemaCatalogEntry &schema) {
        schema.Scan(context,CatalogType::SCALAR_FUNCTION_ENTRY,[&](CatalogEntry &value) {
            if (value.type != CatalogType::SCALAR_FUNCTION_ENTRY) {return;}
            if (!GRV_DATA_SCALARS.count(StringUtil::Lower(value.name))) { return; }
            auto &entry = value.Cast<ScalarFunctionCatalogEntry>();
            for (idx_t index=0; index<entry.functions.Size(); ++index) {
                auto function = entry.functions.GetFunctionByOffset(index);
                if (!function.HasModifiedDatabasesCallback()) { policy->scalars.push_back(function); }
            }
        });
        schema.Scan(context,CatalogType::AGGREGATE_FUNCTION_ENTRY,[&](CatalogEntry &value) {
            if (value.type != CatalogType::AGGREGATE_FUNCTION_ENTRY) {return;}
            auto &entry = value.Cast<AggregateFunctionCatalogEntry>();
            for (idx_t index=0; index<entry.functions.Size(); ++index) { policy->aggregates.push_back(entry.functions.GetFunctionByOffset(index)); }
        });
        schema.Scan(context,CatalogType::TYPE_ENTRY,[&](CatalogEntry &value) {
            if (value.type != CatalogType::TYPE_ENTRY) {return;}
            auto &entry = value.Cast<TypeCatalogEntry>();
            policy->types.push_back({entry.name,entry.user_type,entry.bind_function});
        });
    });
    policy->scalars.push_back(DecodeSortKeyFun::GetFunction());
    for (const string name : {"range", "generate_series"}) {
        auto &entry = catalog.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, name);
        for (idx_t index=0; index<entry.functions.Size(); ++index) { policy->tables.push_back(entry.functions.GetFunctionByOffset(index)); }
    }
    for (const string name : {"read_parquet", "parquet_scan"}) {
        auto &entry = catalog.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, name);
        for (idx_t index=0; index<entry.functions.Size(); ++index) { policy->verified_file_scanners.push_back(entry.functions.GetFunctionByOffset(index)); }
    }
    return policy;
}
}

extern "C" {
// Each function catches C++ exceptions; none cross the FFI boundary.
const char *grv_native_version() { return DuckDB::LibraryVersion(); }
unsigned int grv_native_guard_revision() { return 1; }
// A standalone C-API database uses the same compiled static Parquet extension as
// Engine. This initializer changes no connection state, guard, or configuration
// and deliberately offers no SQL execution seam.
int grv_native_load_static_extensions(duckdb_database database, char *error, size_t capacity) {
    try {
        if (!database) { throw InvalidInputException("static extension initializer requires a live C-API database"); }
        auto &wrapper = *reinterpret_cast<DatabaseWrapper *>(database);
        if (!wrapper.database) { throw InvalidInputException("C-API database is closed"); }
        wrapper.database->LoadStaticExtension<ParquetExtension>();
        return 0;
    } catch (const std::exception &failure) { Error(error, capacity, failure); return -1; }
}
void *grv_native_open_mode(const char *path, bool read_only, char *error, size_t capacity) {
    try {
        DBConfig configuration;
        if (read_only) { configuration.SetOptionByName("access_mode",Value("READ_ONLY")); }
        configuration.SetOptionByName("memory_limit", Value("512MiB"));
        configuration.SetOptionByName("max_temp_directory_size", Value("16GiB"));
        configuration.SetOptionByName("enable_external_access", Value(false));
        configuration.SetOptionByName("autoinstall_known_extensions", Value(false));
        configuration.SetOptionByName("autoload_known_extensions", Value(false));
        configuration.SetOptionByName("threads", Value(int64_t(1)));
        auto engine = std::make_unique<Engine>(path, configuration);
        engine->database.LoadStaticExtension<ParquetExtension>();
        engine->connection.context->config.use_replacement_scans = false;
        shared_ptr<GRVGuard> policy;
        engine->connection.context->RunFunctionInTransaction([&]() { policy = Policy(*engine->connection.context); });
        engine->connection.context->registered_state->Insert("grv.native.guard.v1", policy);
        auto interrupt = std::make_shared<GRVInterruptControl>(); interrupt->engine = engine.get();
        return new Owner{std::move(engine), std::move(interrupt)};
    } catch (const std::exception &failure) { Error(error, capacity, failure); return nullptr; }
}
void *grv_native_open(const char *path,char *error,size_t capacity) { return grv_native_open_mode(path,false,error,capacity); }
void *grv_native_open_readonly(const char *path,char *error,size_t capacity) { return grv_native_open_mode(path,true,error,capacity); }
void grv_native_close(void *owner) {
    auto *value = static_cast<Owner *>(owner);
    auto state = value->interrupt;
    std::lock_guard<std::mutex> guard(state->mutex);
    state->engine = nullptr;
    delete value; // Connections close on the owner thread, after every in-flight interrupt.
}
void *grv_native_interrupt_handle(void *owner) { return new InterruptOwner{static_cast<Owner *>(owner)->interrupt}; }
void grv_native_interrupt(void *handle) {
    auto state = static_cast<InterruptOwner *>(handle)->state;
    std::lock_guard<std::mutex> guard(state->mutex);
    if (state->engine) { state->engine->connection.Interrupt(); }
}
void grv_native_interrupt_close(void *handle) { delete static_cast<InterruptOwner *>(handle); }
int grv_native_begin(void *owner, const char *query, char *error, size_t capacity) {
    try {
        auto &engine = *static_cast<Owner *>(owner)->engine;
        engine.result.reset();
        auto statements = engine.connection.ExtractStatements(query);
        if (statements.size() != 1 || statements[0]->type != StatementType::SELECT_STATEMENT) {
            throw BinderException("GRV queries require exactly one SELECT statement");
        }
        engine.result = engine.connection.SendQuery(std::move(statements[0]));
        if (engine.result->HasError()) { throw std::runtime_error(engine.result->GetError()); }
        // A fixed-width vector of up to 2048 rows remains below 8MiB including
        // validity and the proof encoding. Validate before the first chunk fetch.
        if (engine.result->types.size() > 256) { throw BinderException("native feasibility schema exceeds bounded fetch width"); }
        for (const auto &type : engine.result->types) {
            if (type != LogicalType::BIGINT) { throw BinderException("native feasibility fetch requires int64 columns"); }
        }
        return 0;
    } catch (const std::exception &failure) {
        static_cast<Owner *>(owner)->engine->result.reset();
        Error(error, capacity, failure); return -1;
    }
}
// Fixed-width proof encoding: u64 row count, u64 column count, then one validity
// byte and eight little-endian bytes for each value. Full logical conversion follows later.
int grv_native_fetch(void *owner, unsigned char *output, size_t allowance, size_t *written, char *error, size_t capacity) {
    try {
        auto &engine = *static_cast<Owner *>(owner)->engine;
        if (!engine.result) { throw std::runtime_error("query has not begun"); }
        auto chunk = engine.result->Fetch();
        if (!chunk) {
            if (engine.result->HasError()) { throw std::runtime_error(engine.result->GetError()); }
            *written = 0; return 0;
        }
        size_t rows = chunk->size(), columns = chunk->ColumnCount();
        if (columns > (allowance >= 16 ? (allowance - 16) / 9 : 0) ||
            rows > (allowance >= 16 && columns ? (allowance - 16) / (9 * columns) : 0)) {
            throw std::runtime_error("native chunk exceeds reserved outbound allowance");
        }
        size_t offset = 0;
        auto write_u64 = [&](uint64_t number) { for (size_t index=0; index<8; ++index) { output[offset++] = static_cast<unsigned char>(number >> (8*index)); } };
        write_u64(rows); write_u64(columns);
        for (size_t row=0; row<rows; ++row) { for (size_t column=0; column<columns; ++column) {
            auto value = chunk->GetValue(column, row);
            output[offset++] = value.IsNull() ? 0 : 1;
            write_u64(value.IsNull() ? 0 : static_cast<uint64_t>(value.GetValue<int64_t>()));
        }}
        *written = offset; return 1;
    } catch (const std::exception &failure) { Error(error, capacity, failure); return -1; }
}
}

#include "staging.inc"
#include "pull.inc"

#include "s3.inc"
