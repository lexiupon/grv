#pragma once
// Compiled into the pinned engine at its binding sites. These checks run BEFORE
// callbacks, rather than inspecting an already-bound plan. The same sites run on rebind.
#include "duckdb.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include <set>
#include <map>

namespace duckdb {
class GRVGuard : public ClientContextState {
public:
    vector<ScalarFunction> scalars;
    vector<TableFunction> tables;
    vector<TableFunction> verified_file_scanners;
    vector<AggregateFunction> aggregates;
    struct TypeIdentity { string name; LogicalType type; bind_logical_type_function_t bind; };
    vector<TypeIdentity> types;
    std::set<string> relations;
    std::map<string,string> relation_catalogs;
    std::set<string> private_relations;
    bool internal_scan = false;
    bool metadata_scan = false;
    string metadata_catalog;
    static shared_ptr<GRVGuard> Get(ClientContext &context) { return context.registered_state->Get<GRVGuard>("grv.native.guard.v1"); }
    static void Denied(const string &name) { throw BinderException("GRV native guard rejected dependency: %s", name); }
    static bool SameScalarCallback(const scalar_function_t &left, const scalar_function_t &right) {
        if (!left || !right) { return !left && !right; }
        using Pointer = void (*)(DataChunk &, ExpressionState &, Vector &);
        auto l = left.target<Pointer>(); auto r = right.target<Pointer>();
        // Capturing callables need a separately reviewed identity representation.
        return l && r && *l == *r;
    }

    bool ScalarAllowed(const ScalarFunction &value) {
        for (const auto &known : scalars) {
            if (known.name == value.name && SameScalarCallback(known.function, value.function) &&
                known.GetBindCallback() == value.GetBindCallback() && known.GetBindExtendedCallback() == value.GetBindExtendedCallback() &&
                known.GetBindLambdaCallback() == value.GetBindLambdaCallback() && known.GetBindExpressionCallback() == value.GetBindExpressionCallback() &&
                known.GetModifiedDatabasesCallback() == value.GetModifiedDatabasesCallback() && known.GetStatisticsCallback() == value.GetStatisticsCallback() &&
                known.GetInitStateCallback() == value.GetInitStateCallback() && known.function_info == value.function_info) { return true; }
        }
        return false;
    }
    bool TableAllowed(const TableFunction &value) {
        for (const auto &known : tables) {
            if (known == value && known.function_info == value.function_info && known.statistics_extended == value.statistics_extended &&
                known.rows_scanned == value.rows_scanned && known.get_metrics == value.get_metrics && known.supports_pushdown_extract == value.supports_pushdown_extract) { return true; }
        }
        return false;
    }
    static bool SameAggregate(const AggregateFunction &known,const AggregateFunction &value) {
            return known.name == value.name && known.bind == value.bind && known.state_size == value.state_size &&
                known.initialize == value.initialize && known.update == value.update && known.combine == value.combine &&
                known.finalize == value.finalize && known.simple_update == value.simple_update && known.window == value.window &&
                known.window_init == value.window_init && known.destructor == value.destructor && known.statistics == value.statistics &&
                known.function_info == value.function_info;
    }
    bool AggregateAllowed(const AggregateFunction &value) {
        for (const auto &known : aggregates) {
            if (SameAggregate(known,value)) { return true; }
        }
        // The pinned binder constructs typed first/last descriptors directly for
        // scalar subqueries (including generated metadata INSERT subqueries).
        // Their callbacks must match the exact native factory, never just names.
        if (value.arguments.size()==1) {
            if (value.name=="first" && SameAggregate(FirstFunctionGetter::GetFunction(value.arguments[0]),value)) {return true;}
            if (value.name=="last" && SameAggregate(LastFunctionGetter::GetFunction(value.arguments[0]),value)) {return true;}
        }
        return false;
    }
    static void Scalar(ClientContext &context, const ScalarFunction &function) {
        auto policy = Get(context); if (policy && !policy->ScalarAllowed(function)) { Denied(function.name); }
    }
    static void Table(ClientContext &context, const TableFunction &function) {
        auto policy = Get(context); if (policy && !policy->TableAllowed(function)) { Denied(function.name); }
    }
    static void Aggregate(ClientContext &context, const AggregateFunction &function) {
        auto policy = Get(context); if (policy && !policy->AggregateAllowed(function)) { Denied(function.name); }
    }
    static void Replacement(ClientContext &context) { if (Get(context)) { Denied("replacement scan"); } }
    static void CustomCast(optional_ptr<ClientContext> context) {
        if (context && Get(*context)) { Denied("registered custom cast"); }
    }
    static void Entry(ClientContext &context, CatalogEntry &entry) {
        auto policy = Get(context); if (!policy) { return; }
        // Table binding invokes virtual scan callbacks after catalog lookup. Only
        // the pinned engine's native storage table class is authorized here.
        if (entry.type == CatalogType::TABLE_ENTRY && dynamic_cast<DuckTableEntry *>(&entry) == nullptr) {
            Denied(entry.name);
        }
        switch (entry.type) {
        case CatalogType::SCALAR_FUNCTION_ENTRY: {
            auto &functions = entry.Cast<ScalarFunctionCatalogEntry>().functions;
            for (idx_t index=0; index<functions.Size(); ++index) { Scalar(context, functions.GetFunctionReferenceByOffset(index)); }
            return;
        }
        case CatalogType::TABLE_FUNCTION_ENTRY: {
            auto &functions = entry.Cast<TableFunctionCatalogEntry>().functions;
            for (idx_t index=0; index<functions.Size(); ++index) { Table(context, functions.GetFunctionReferenceByOffset(index)); }
            return;
        }
        case CatalogType::AGGREGATE_FUNCTION_ENTRY: {
            auto &functions = entry.Cast<AggregateFunctionCatalogEntry>().functions;
            for (idx_t index=0; index<functions.Size(); ++index) { Aggregate(context, functions.GetFunctionReferenceByOffset(index)); }
            return;
        }
        case CatalogType::TYPE_ENTRY: {
            auto &type = entry.Cast<TypeCatalogEntry>();
            for (const auto &known : policy->types) {
                if (StringUtil::CIEquals(known.name, type.name) && known.type == type.user_type && known.bind == type.bind_function) { return; }
            }
            break;
        }
        case CatalogType::TABLE_ENTRY: case CatalogType::VIEW_ENTRY: case CatalogType::MACRO_ENTRY: case CatalogType::TABLE_MACRO_ENTRY:
            if (policy->metadata_scan && entry.type == CatalogType::TABLE_ENTRY && entry.ParentCatalog().GetName() == policy->metadata_catalog &&
                StringUtil::CIEquals(entry.ParentSchema().name,"_grv") &&
                std::set<string>{"metadata_header","relation_ownership","workspace_binding","pull_attempts","replacement_scopes","pull_checkpoint","pull_meta","import_bindings","import_attempt_details","build_sessions"}.count(entry.name)) { return; }
            if (policy->internal_scan && entry.ParentCatalog().GetName() == "temp" && policy->private_relations.count(entry.ParentSchema().name + "." + entry.name)) { return; }
            if (!StringUtil::CIEquals(entry.ParentSchema().name, "_grv") && policy->relations.count(StringUtil::Lower(entry.ParentSchema().name + "." + entry.name))) {
                auto catalog = policy->relation_catalogs.find(StringUtil::Lower(entry.ParentSchema().name + "." + entry.name));
                if (catalog == policy->relation_catalogs.end() || catalog->second == entry.ParentCatalog().GetName()) { return; }
            }
            break;
        default: break;
        }
        Denied(entry.name);
    }
};
}
