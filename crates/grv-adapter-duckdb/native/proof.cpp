#include "grv_guard.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include <atomic>
#include <iostream>
#include <stdexcept>

using namespace duckdb;
static std::atomic<size_t> table_binds{0}, scalar_binds{0}, aggregate_binds{0}, replacement_calls{0}, cast_binds{0}, type_binds{0};
static void Require(bool condition, const char *message) { if (!condition) { throw std::runtime_error(message); } }
static unique_ptr<FunctionData> EvilTableBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types, vector<string> &names) {
    table_binds++; types.push_back(LogicalType::BIGINT); names.push_back("value"); return nullptr;
}
static void EvilTable(ClientContext &, TableFunctionInput &, DataChunk &output) { output.SetCardinality(0); }
static unique_ptr<FunctionData> EvilScalarBind(ClientContext &, ScalarFunction &, vector<unique_ptr<Expression>> &) { scalar_binds++; return nullptr; }
static unique_ptr<FunctionData> EvilAggregateBind(ClientContext &, AggregateFunction &, vector<unique_ptr<Expression>> &) { aggregate_binds++; return nullptr; }
static unique_ptr<TableRef> EvilReplacement(ClientContext &, ReplacementScanInput &, optional_ptr<ReplacementScanData>) { replacement_calls++; return nullptr; }
static BoundCastInfo EvilCastBind(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
    cast_binds++; return DefaultCasts::GetDefaultCastFunction(input, source, target);
}
static LogicalType EvilTypeBind(BindLogicalTypeInput &) { type_binds++; return LogicalType::BIGINT; }
static void EvilScalar(DataChunk &, ExpressionState &, Vector &output) {
    output.SetVectorType(VectorType::CONSTANT_VECTOR); ConstantVector::GetData<int64_t>(output)[0] = 42;
}
static void Good(Connection &connection, const string &query) {
    auto result = connection.Query(query);
    if (result->HasError()) { throw std::runtime_error(result->GetError()); }
}
static void Rejected(Connection &connection, const string &query) {
    auto result = connection.Query(query);
    Require(result->HasError(), "forbidden query was allowed");
    Require(result->GetError().find("GRV native guard") != string::npos, "query failed outside the native guard");
    Require(table_binds == 0 && scalar_binds == 0, "forbidden bind-time callback executed");
}

int main(int argc, char **argv) {
    try {
        const string scenario = argc > 1 ? argv[1] : "all";
        Require(std::set<string>{"all", "initial", "nested", "metadata", "rebind", "direct", "cast", "type"}.count(scenario), "unknown proof scenario");
        auto run = [&](const char *name) { return scenario == "all" || scenario == name; };
        Require(string(DuckDB::LibraryVersion()) == "v1.5.6", "engine version mismatch");
        DBConfig configuration;
        configuration.SetOptionByName("memory_limit", Value("512MiB"));
        configuration.SetOptionByName("enable_external_access", Value(false));
        configuration.SetOptionByName("autoinstall_known_extensions", Value(false));
        configuration.SetOptionByName("autoload_known_extensions", Value(false));
        configuration.replacement_scans.emplace_back(EvilReplacement);
        DuckDB database(nullptr, &configuration); Connection connection(database);
        ExtensionLoader loader(*database.instance, "grv_native_canaries");
        loader.RegisterFunction(TableFunction("evil_table", {}, EvilTable, EvilTableBind));
        loader.RegisterFunction(ScalarFunction("evil_scalar", {}, LogicalType::BIGINT, EvilScalar, EvilScalarBind));
        loader.RegisterType("evil_type",LogicalType::BIGINT,EvilTypeBind);
        Good(connection,"SELECT CAST(42 AS evil_type)");
        Require(type_binds > 0,"type callback canary was not live"); type_binds = 0;
        Good(connection, "CREATE VIEW nested AS SELECT * FROM evil_table()");
        Good(connection, "CREATE MACRO wrapped() AS TABLE SELECT evil_scalar() AS value");
        Good(connection, "CREATE MACRO outer_wrapped() AS TABLE SELECT * FROM wrapped()");
        Good(connection, "CREATE TABLE source(value BIGINT); INSERT INTO source VALUES (3)");
        Good(connection, "CREATE TABLE int_source(value INTEGER); INSERT INTO int_source VALUES (3)");
        Good(connection, "CREATE VIEW changing AS SELECT value FROM source");
        Good(connection, "CREATE SCHEMA _grv; CREATE TABLE _grv.secret(value BIGINT)");
        Good(connection, "SELECT evil_scalar()");
        Good(connection, "SELECT * FROM evil_table()");
        loader.RegisterCastFunction(LogicalType::INTEGER, LogicalType::BIGINT, EvilCastBind, 0);
        Good(connection, "SELECT value::BIGINT FROM int_source");
        Require(cast_binds > 0, "custom cast canary was not live");
        cast_binds = 0;
        Require(table_binds > 0 && scalar_binds > 0, "callback canaries were not live");
        table_binds = 0; scalar_binds = 0;
        auto policy = make_shared_ptr<GRVGuard>();
        Good(connection, "BEGIN");
        auto &range = Catalog::GetSystemCatalog(*connection.context).GetEntry<TableFunctionCatalogEntry>(*connection.context, DEFAULT_SCHEMA, "range");
        for (idx_t index=0; index<range.functions.Size(); ++index) { policy->tables.push_back(range.functions.GetFunctionByOffset(index)); }
        auto &count = Catalog::GetSystemCatalog(*connection.context).GetEntry<AggregateFunctionCatalogEntry>(*connection.context, DEFAULT_SCHEMA, "count");
        for (idx_t index=0; index<count.functions.Size(); ++index) { policy->aggregates.push_back(count.functions.GetFunctionByOffset(index)); }
        auto &addition = Catalog::GetSystemCatalog(*connection.context).GetEntry<ScalarFunctionCatalogEntry>(*connection.context, DEFAULT_SCHEMA, "+");
        for (idx_t index=0; index<addition.functions.Size(); ++index) { policy->scalars.push_back(addition.functions.GetFunctionByOffset(index)); }
        policy->relations = {"main.nested", "main.wrapped", "main.outer_wrapped", "main.changing", "main.source", "main.int_source", "_grv.secret"};
        Good(connection, "COMMIT");
        connection.context->registered_state->Insert("grv.native.guard.v1", policy);
        if (run("initial")) {
            Rejected(connection, "SELECT * FROM evil_table()");
            Rejected(connection, "SELECT evil_scalar()");
            std::cout << "PASS initial table/scalar bind guard\n";
        }
        if (run("nested")) {
            Rejected(connection, "SELECT * FROM nested");
            Rejected(connection, "SELECT * FROM outer_wrapped()");
            std::cout << "PASS nested view/macro expansion guard\n";
        }
        if (run("metadata")) {
            Rejected(connection, "SELECT * FROM _grv.secret");
            Rejected(connection, "SELECT * FROM missing_replacement_scan");
            Require(replacement_calls == 0, "replacement callback ran before guard");
            std::cout << "PASS metadata and replacement callback guard\n";
        }
        if (run("cast")) {
            Rejected(connection, "SELECT value::BIGINT FROM int_source");
            Require(cast_binds == 0, "explicit cast bind callback executed");
            Rejected(connection, "SELECT value + 2::BIGINT FROM int_source");
            Require(cast_binds == 0, "implicit cast bind callback executed");
            std::cout << "PASS explicit and implicit registered cast callbacks guarded\n";
        }
        if (run("type")) {
            Rejected(connection,"SELECT CAST(42 AS evil_type)");
            Require(type_binds == 0,"type modifier bind callback executed");
            std::cout << "PASS type modifier callbacks guarded before binding\n";
        }

        if (run("rebind")) {
        auto prepared = connection.Prepare("SELECT * FROM changing");
        Require(!prepared->HasError(), "allowed initial prepare failed");
        Require(!prepared->GetStatementProperties().read_databases.empty(), "rebind canary must depend on local catalog identity");
        connection.context->registered_state->Remove("grv.native.guard.v1");
        Good(connection, "CREATE OR REPLACE VIEW changing AS SELECT * FROM evil_table()");
        table_binds = 0; scalar_binds = 0;
        connection.context->registered_state->Insert("grv.native.guard.v1", policy);
        auto rebound = prepared->Execute();
        Require(rebound->HasError() && rebound->GetError().find("GRV native guard") != string::npos, "automatic rebind bypassed native guard");
        Require(table_binds == 0 && scalar_binds == 0, "automatic rebind ran forbidden callback");
        std::cout << "PASS automatic rebind rejects before bind-time callback\n";
        }

        if (run("direct")) {
        // A direct native binder call must not bypass catalog checking.
        auto evil = ScalarFunction("evil_scalar", {}, LogicalType::BIGINT, EvilScalar, EvilScalarBind);
        bool denied = false;
        try { FunctionBinder binder(*connection.context); binder.BindScalarFunction(evil, {}, false); }
        catch (const std::exception &error) { denied = string(error.what()).find("GRV native guard") != string::npos; }
        Require(denied && scalar_binds == 0, "direct native function bind bypassed guard");
        auto forged = range.functions.GetFunctionByOffset(0);
        forged.bind = EvilTableBind;
        denied = false;
        try { GRVGuard::Table(*connection.context, forged); }
        catch (const std::exception &error) { denied = string(error.what()).find("GRV native guard") != string::npos; }
        Require(denied, "forged approved function name was accepted");
        auto evil_aggregate = count.functions.GetFunctionByOffset(0);
        evil_aggregate.bind = EvilAggregateBind;
        denied = false;
        try { FunctionBinder binder(*connection.context); binder.BindAggregateFunction(evil_aggregate, {}, nullptr, AggregateType::NON_DISTINCT); }
        catch (const std::exception &error) { denied = string(error.what()).find("GRV native guard") != string::npos; }
        Require(denied && aggregate_binds == 0, "direct native aggregate bind bypassed guard");
        auto forged_first = FirstFunctionGetter::GetFunction(LogicalType::VARCHAR);
        forged_first.bind = EvilAggregateBind;
        denied = false;
        vector<unique_ptr<Expression>> first_children;
        first_children.push_back(make_uniq<BoundConstantExpression>(Value("value")));
        try { FunctionBinder binder(*connection.context); binder.BindAggregateFunction(forged_first, std::move(first_children), nullptr, AggregateType::NON_DISTINCT); }
        catch (const std::exception &error) { denied = string(error.what()).find("GRV native guard") != string::npos; }
        Require(denied && aggregate_binds == 0, "typed first factory accepted forged callbacks");
        std::cout << "PASS direct native function binding and callback identity checks\n";
        }
        return 0;
    } catch (const std::exception &failure) { std::cerr << failure.what() << '\n'; return 1; }
}
