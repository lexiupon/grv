// Internal acquisition seam. This is deliberately not a public SQL command.
// The SELECT is evaluated once; only engine-owned staging retains its result.
namespace {
struct InternalScan {
    shared_ptr<GRVGuard> policy;
    explicit InternalScan(Engine &engine) : policy(GRVGuard::Get(*engine.connection.context)) { policy->internal_scan = true; }
    ~InternalScan() { policy->internal_scan = false; }
};
unique_ptr<MaterializedQueryResult> CheckedQuery(Engine &engine, const string &query) {
    auto result = engine.connection.Query(query);
    if (result->HasError()) { throw std::runtime_error(result->GetError()); }
    return result;
}
unsigned TypeCode(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::BOOLEAN: return 1;
    case LogicalTypeId::TINYINT: return 2;
    case LogicalTypeId::SMALLINT: return 3;
    case LogicalTypeId::INTEGER: return 4;
    case LogicalTypeId::BIGINT: return 5;
    case LogicalTypeId::FLOAT: return 6;
    case LogicalTypeId::DOUBLE: return 7;
    case LogicalTypeId::VARCHAR: return 8;
    case LogicalTypeId::BLOB: return 9;
    case LogicalTypeId::DATE: return 10;
    case LogicalTypeId::DECIMAL: return 11;
    case LogicalTypeId::TIMESTAMP_SEC: return 12;
    case LogicalTypeId::TIMESTAMP_MS: return 13;
    case LogicalTypeId::TIMESTAMP: return 14;
    case LogicalTypeId::TIMESTAMP_NS: return 15;
    case LogicalTypeId::TIMESTAMP_TZ: return 16;
    default: throw BinderException("unsupported exact GRV native source type: %s", type.ToString());
    }
}
size_t Width(unsigned code) {
    switch (code) { case 1: return 1; case 6: case 10: return 4; case 11: return 16; default: return 8; }
}
string Column(idx_t index) { return "\"_grv_c" + std::to_string(index) + "\""; }
string Window(const AcquiredTable &stage, idx_t start, idx_t rows) {
    return " FROM temp.main.\"" + stage.name + "\" WHERE \"_grv_ordinal\" >= " + std::to_string(start) +
           " AND \"_grv_ordinal\" < " + std::to_string(start + rows) + " ORDER BY \"_grv_ordinal\"";
}
void Put(unsigned char *output, size_t &offset, uint64_t number, size_t width) {
    for (size_t index=0; index<width; ++index) { output[offset++] = static_cast<unsigned char>(number >> (index*8)); }
}
string Quote(const string &identifier) { return "\"" + StringUtil::Replace(identifier,"\"","\"\"") + "\""; }
void RowExpression(const ParsedExpression &expression) {
    if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY || expression.GetExpressionClass() == ExpressionClass::WINDOW ||
        expression.GetExpressionClass() == ExpressionClass::STAR || expression.GetExpressionClass() == ExpressionClass::PARAMETER) {
        throw BinderException("extraction filter must be one row predicate without relation reads");
    }
    ParsedExpressionIterator::EnumerateChildren(expression,[&](const ParsedExpression &child) { RowExpression(child); });
}
string Predicate(const char *text) {
    if (!text || !*text) { return ""; }
    Parser parser;
    parser.ParseQuery("SELECT " + string(text));
    if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) { throw BinderException("filter must be exactly one predicate"); }
    auto &query = parser.statements[0]->Cast<SelectStatement>();
    if (query.node->type != QueryNodeType::SELECT_NODE) { throw BinderException("filter cannot contain a query"); }
    auto &node = query.node->Cast<SelectNode>();
    if (node.select_list.size() != 1 || node.from_table->type != TableReferenceType::EMPTY_FROM || node.where_clause || node.having || node.qualify || node.sample ||
        !node.groups.group_expressions.empty() || !node.modifiers.empty() || !node.cte_map.map.empty()) { throw BinderException("filter must be exactly one row predicate"); }
    if (node.select_list[0]->HasAlias()) {throw BinderException("filter cannot define a SELECT alias");}
    RowExpression(*node.select_list[0]);
    return " WHERE " + node.select_list[0]->ToString();
}
struct MetadataScan {
    shared_ptr<GRVGuard> policy;
    MetadataScan(Engine &engine,const string &catalog) : policy(GRVGuard::Get(*engine.connection.context)) {policy->metadata_catalog=catalog;policy->metadata_scan=true;}
    ~MetadataScan() {policy->metadata_scan=false;}
};
void UnmanagedSource(Engine &engine,const string &catalog,const string &schema,const string &table) {
    auto &source_catalog = Catalog::GetCatalog(*engine.connection.context,catalog);
    if (!source_catalog.GetSchema(*engine.connection.context,"_grv",OnEntryNotFound::RETURN_NULL)) {return;}
    MetadataScan inspection(engine,catalog);
    const string prefix = Quote(catalog) + ".\"_grv\".";
    auto &header_entry = Catalog::GetEntry(*engine.connection.context,catalog,"_grv",EntryLookupInfo(CatalogType::TABLE_ENTRY,"metadata_header"));
    auto &ownership_entry = Catalog::GetEntry(*engine.connection.context,catalog,"_grv",EntryLookupInfo(CatalogType::TABLE_ENTRY,"relation_ownership"));
    if (header_entry.type != CatalogType::TABLE_ENTRY || ownership_entry.type != CatalogType::TABLE_ENTRY) {throw BinderException("ambiguous consumer metadata ownership format");}
    auto header = CheckedQuery(engine,"SELECT version,ownership_version FROM " + prefix + "\"metadata_header\" LIMIT 2");
    if (header->RowCount() != 1 || header->types != vector<LogicalType>{LogicalType::BIGINT,LogicalType::BIGINT} ||
        header->GetValue(0,0).IsNull() || header->GetValue(1,0).IsNull() || header->GetValue(0,0).GetValue<int64_t>() != 1 || header->GetValue(1,0).GetValue<int64_t>() != 1) {
        throw BinderException("unsupported consumer metadata ownership version");
    }
    auto owned = CheckedQuery(engine,"SELECT schema_name,table_name,kind FROM " + prefix + "\"relation_ownership\" LIMIT 0");
    if (owned->types != vector<LogicalType>{LogicalType::VARCHAR,LogicalType::VARCHAR,LogicalType::VARCHAR}) {throw BinderException("invalid consumer relation ownership schema");}
    auto corrupt = CheckedQuery(engine,"SELECT count(*)::BIGINT FROM " + prefix + "\"relation_ownership\" WHERE schema_name IS NULL OR table_name IS NULL OR kind IS NULL");
    if (corrupt->GetValue(0,0).GetValue<int64_t>() != 0) {throw BinderException("incomplete consumer relation ownership evidence");}
    auto literal = [](const string &value) {return "'" + StringUtil::Replace(StringUtil::Lower(value),"'","''") + "'";};
    auto managed = CheckedQuery(engine,"SELECT count(*)::BIGINT FROM " + prefix + "\"relation_ownership\" WHERE lower(schema_name)=" + literal(schema) + " AND lower(table_name)=" + literal(table));
    if (managed->GetValue(0,0).GetValue<int64_t>() != 0) {throw BinderException("managed imports and GRV-derived working tables cannot be extraction sources");}
}
}
extern "C" {
int grv_native_authorize_relation(void *owner, const char *schema, const char *table, char *error, size_t capacity) {
    try {
        auto &engine = *static_cast<Owner *>(owner)->engine;
        string schema_name(schema), table_name(table);
        if (engine.staged || StringUtil::CIEquals(schema_name,"_grv")) {
            throw BinderException("reserved or frozen source relation authorization");
        }
        auto policy = GRVGuard::Get(*engine.connection.context);
        auto qualified = StringUtil::Lower(schema_name + "." + table_name);
        policy->relations.insert(qualified);
        policy->relation_catalogs[qualified] = DatabaseManager::GetDefaultDatabase(*engine.connection.context);
        return 0;
    } catch (const std::exception &failure) { Error(error, capacity, failure); return -1; }
}
int grv_native_stage_index(void *owner, uint64_t table_id, const char *query, unsigned char *schema_output, size_t schema_allowance,
                     size_t *schema_written, uint64_t *rows, char *error, size_t capacity) {
    try {
        auto &engine = *static_cast<Owner *>(owner)->engine;
        if ((!engine.multi_acquisition && engine.staged) || engine.acquisition_sealed || table_id != engine.stages.size()) { throw BinderException("acquisition cannot be repeated on this owner"); }
        engine.stages.emplace_back();
        auto &stage = engine.stages.back();
        stage.name = "_grv_acquisition_" + std::to_string(table_id);
        // Freeze before any source effect, including a failed/ambiguous acquisition.
        engine.staged = true;
        auto statements = engine.connection.ExtractStatements(query);
        if (statements.size() != 1 || statements[0]->type != StatementType::SELECT_STATEMENT) {
            throw BinderException("acquisition requires exactly one SELECT");
        }
        string selected = statements[0]->ToString(); // Re-serialized parsed SELECT is safe to embed.
        if (!engine.snapshot_started) { CheckedQuery(engine,"BEGIN TRANSACTION"); engine.snapshot_started = true; }
        auto prepared = engine.connection.Prepare(selected);
        if (prepared->HasError()) { throw BinderException(prepared->GetError()); }
        stage.types = prepared->GetTypes();
        if (stage.types.empty() || schema_allowance < 4 + 6 * stage.types.size()) {
            throw BinderException("bounded acquisition schema exceeds column allowance");
        }
        string columns;
        size_t offset = 0;
        Put(schema_output, offset, stage.types.size(), 4);
        for (idx_t index=0; index<stage.types.size(); ++index) {
            const auto &type = stage.types[index];
            Put(schema_output, offset, TypeCode(type), 4);
            Put(schema_output, offset, type.id() == LogicalTypeId::DECIMAL ? DecimalType::GetWidth(type) : 0, 1);
            Put(schema_output, offset, type.id() == LogicalTypeId::DECIMAL ? DecimalType::GetScale(type) : 0, 1);
            if (index) { columns += ","; } columns += Column(index);
        }
        // CTAS stores source bytes under the separately configured engine memory/spill quota.
        CheckedQuery(engine, "CREATE TEMP TABLE \"" + stage.name + "\" AS SELECT " + columns +
            ", row_number() OVER () - 1 AS \"_grv_ordinal\" FROM (" + selected + ") AS \"_grv_source\" (" + columns + ")");
        auto policy = GRVGuard::Get(*engine.connection.context);
        policy->private_relations.insert("main." + stage.name);
        InternalScan internal(engine);
        auto count = CheckedQuery(engine, "SELECT count(*)::BIGINT FROM temp.main.\"" + stage.name + "\"");
        stage.rows = static_cast<idx_t>(count->GetValue(0, 0).GetValue<int64_t>());
        stage.complete = true;
        *rows = stage.rows; *schema_written = offset;
        return 0;
    } catch (const std::exception &failure) { Error(error, capacity, failure); return -1; }
}
int grv_native_stage_fetch_index(void *owner, uint64_t table_id, unsigned char *output, size_t allowance, size_t source_allowance,
                           size_t *written, char *error, size_t capacity) {
    try {
        auto &engine = *static_cast<Owner *>(owner)->engine;
        if (!engine.staged || table_id >= engine.stages.size() || (engine.multi_acquisition && !engine.acquisition_sealed)) { throw BinderException("acquisition is unavailable"); }
        auto &stage = engine.stages[table_id];
        if (stage.types.empty()) { throw BinderException("acquisition failed before schema validation"); }
        if (stage.cursor == stage.rows) { *written = 0; return 0; }
        InternalScan internal(engine);
        idx_t candidate = std::min<idx_t>(2048, stage.rows-stage.cursor);
        string lengths;
        for (idx_t column=0; column<stage.types.size(); ++column) {
            if (column) { lengths += ","; }
            unsigned code = TypeCode(stage.types[column]);
            lengths += "CASE WHEN " + Column(column) + " IS NULL THEN 0::BIGINT ELSE ";
            lengths += code == 8 ? "strlen(" + Column(column) + ")" : code == 9 ? "octet_length(" + Column(column) + ")" : std::to_string(Width(code)) + "::BIGINT";
            lengths += " END";
        }
        // Length metadata itself is bounded before its fixed-width native fetch.
        size_t metadata_row = 64 * stage.types.size() + 512;
        candidate = std::min<idx_t>(candidate, source_allowance / metadata_row);
        if (!candidate) { throw BinderException("source unit cannot hold bounded length metadata"); }
        auto metadata = CheckedQuery(engine, "SELECT " + lengths + Window(stage, stage.cursor, candidate));
        // Native vectors retain standard-capacity allocation even for a short
        // final window. Reserve it BEFORE asking Query to materialize data.
        const size_t vector_overhead = 65536 * stage.types.size() + 65536;
        if (source_allowance < vector_overhead) { throw BinderException("source unit cannot hold bounded native vector capacity"); }
        idx_t adopted = 0; size_t encoded = 16, source = vector_overhead;
        for (idx_t row=0; row<metadata->RowCount(); ++row) {
            size_t row_encoded = stage.types.size(), row_source = metadata_row;
            for (idx_t column=0; column<stage.types.size(); ++column) {
                auto bytes = metadata->GetValue(column,row).GetValue<int64_t>();
                if (bytes < 0 || static_cast<uint64_t>(bytes) > allowance) { throw BinderException("source row exceeds outbound allowance"); }
                unsigned code = TypeCode(stage.types[column]);
                size_t length = static_cast<size_t>(bytes);
                row_encoded += length + ((code == 8 || code == 9) ? 8 : (length ? 0 : Width(code)));
                // One native chunk plus a per-cell Value copy; metadata is released before data fetch.
                row_source += 2 * length;
            }
            if (row_encoded > allowance - std::min(encoded,allowance) || row_source > source_allowance - std::min(source,source_allowance)) {
                if (!adopted) { throw BinderException("source row exceeds negotiated transfer budget"); }
                break;
            }
            encoded += row_encoded; source += row_source; ++adopted;
        }
        metadata.reset();
        if (!adopted) { throw BinderException("invalid empty acquisition scan window"); }
        // Only now may a variable-width result enter transfer buffers.
        string projected;
        for (idx_t column=0; column<stage.types.size(); ++column) { if (column) { projected += ","; } projected += Column(column); }
        auto values = CheckedQuery(engine, "SELECT " + projected + Window(stage, stage.cursor, adopted));
        size_t offset = 0;
        Put(output, offset, adopted, 8); Put(output, offset, stage.types.size(), 8);
        for (idx_t row=0; row<adopted; ++row) { for (idx_t column=0; column<stage.types.size(); ++column) {
            auto value = values->GetValue(column,row); unsigned code = TypeCode(stage.types[column]);
            output[offset++] = value.IsNull() ? 0 : 1;
            if (code == 8 || code == 9) {
                const string empty; const auto &bytes = value.IsNull() ? empty : StringValue::Get(value);
                Put(output, offset, bytes.size(), 8); std::memcpy(output+offset,bytes.data(),bytes.size()); offset += bytes.size();
            } else if (value.IsNull()) { for (size_t byte=0; byte<Width(code); ++byte) { output[offset++] = 0; }
            } else if (code == 1) { Put(output,offset,value.GetValue<bool>(),1);
            } else if (code >= 2 && code <= 5) { Put(output,offset,value.GetValue<int64_t>(),8);
            } else if (code == 6) { float number=value.GetValue<float>(); uint32_t bits; std::memcpy(&bits,&number,4); Put(output,offset,bits,4);
            } else if (code == 7) { double number=value.GetValue<double>(); uint64_t bits; std::memcpy(&bits,&number,8); Put(output,offset,bits,8);
            } else if (code == 10) { Put(output,offset,value.GetValue<date_t>().days,4);
            } else if (code == 11) { auto number = IntegralValue::Get(value); Put(output,offset,number.lower,8); Put(output,offset,number.upper,8);
            } else {
                int64_t ticks = code == 12 ? value.GetValue<timestamp_sec_t>().value : code == 13 ? value.GetValue<timestamp_ms_t>().value : code == 15 ? value.GetValue<timestamp_ns_t>().value : code == 16 ? value.GetValue<timestamp_tz_t>().value : value.GetValue<timestamp_t>().value;
                Put(output,offset,ticks,8);
            }
        }}
        if (offset > allowance) { throw InternalException("preflight disagrees with exact native window size"); }
        stage.cursor += adopted; *written = offset; return 1;
    } catch (const std::exception &failure) { Error(error, capacity, failure); return -1; }
}
int grv_native_stage(void *owner, const char *query, unsigned char *schema, size_t allowance,
                     size_t *written, uint64_t *rows, char *error, size_t capacity) {
    return grv_native_stage_index(owner,0,query,schema,allowance,written,rows,error,capacity);
}
int grv_native_stage_fetch(void *owner, unsigned char *output, size_t allowance, size_t source_allowance,
                           size_t *written, char *error, size_t capacity) {
    return grv_native_stage_fetch_index(owner,0,output,allowance,source_allowance,written,error,capacity);
}
int grv_native_snapshot_begin(void *owner,char *error,size_t capacity) {
    try {
        auto &engine = *static_cast<Owner *>(owner)->engine;
        if (engine.staged || engine.snapshot_started) { throw BinderException("snapshot acquisition cannot be repeated"); }
        engine.staged = true; engine.multi_acquisition = true;
        CheckedQuery(engine,"BEGIN TRANSACTION"); engine.snapshot_started = true; return 0;
    } catch (const std::exception &failure) { Error(error,capacity,failure); return -1; }
}
int grv_native_snapshot_seal(void *owner,char *error,size_t capacity) {
    try {
        auto &engine = *static_cast<Owner *>(owner)->engine;
        if (!engine.multi_acquisition || engine.acquisition_sealed || engine.stages.empty()) { throw BinderException("invalid snapshot acquisition seal"); }
        for (const auto &stage : engine.stages) { if (!stage.complete) { throw BinderException("incomplete acquisition member"); } }
        engine.acquisition_sealed = true; return 0;
    } catch (const std::exception &failure) { Error(error,capacity,failure); return -1; }
}
int grv_native_stage_source(void *owner,uint64_t table_id,const char *source_schema,const char *source_table,
                            const char **columns,size_t column_count,const char *filter,
                            unsigned char *schema,size_t allowance,size_t *written,uint64_t *rows,char *error,size_t capacity) {
    try {
        auto &engine = *static_cast<Owner *>(owner)->engine;
        if (!engine.multi_acquisition || !engine.snapshot_started || engine.acquisition_sealed || !column_count) { throw BinderException("invalid source acquisition state"); }
        auto source_namespace=StringUtil::Lower(source_schema);
        if (std::set<string>{"_grv","grv_source","grv_target","grv_input","grv_self"}.count(source_namespace) || source_namespace.find("_grv_session_")==0) {throw BinderException("reserved extraction source namespace");}
        string catalog = DatabaseManager::GetDefaultDatabase(*engine.connection.context);
        auto &entry = Catalog::GetEntry(*engine.connection.context,catalog,string(source_schema),EntryLookupInfo(CatalogType::TABLE_ENTRY,string(source_table)));
        if (entry.type != CatalogType::TABLE_ENTRY || entry.ParentCatalog().GetName() != catalog || StringUtil::CIEquals(source_schema,"_grv")) { throw BinderException("extraction requires an unmanaged local base table"); }
        UnmanagedSource(engine,catalog,source_schema,source_table);
        string projection;
        for (size_t index=0; index<column_count; ++index) { if (index) { projection += ","; } projection += Quote(columns[index]); }
        string selected = "SELECT " + projection + " FROM " + Quote(catalog) + "." + Quote(source_schema) + "." + Quote(source_table) + Predicate(filter);
        return grv_native_stage_index(owner,table_id,selected.c_str(),schema,allowance,written,rows,error,capacity);
    } catch (const std::exception &failure) { Error(error,capacity,failure); return -1; }
}
}
