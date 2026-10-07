// Trusted adapter metadata only. User SQL NEVER enters this FFI. Its callers
// generate closed statements with quoted validated identifiers/literals.
namespace {
// A two-part local schema name can equal DuckDB's filename-derived catalog.
// Token positions avoid touching literals, comments or recorded request JSON.
// Explicit three-part references are already unambiguous and remain unchanged.
string QualifyCatalogCollision(Engine &engine,const string &query) {
    auto tokens=Parser::Tokenize(query);
    // ROLLBACK must remain available after a query aborts the transaction;
    // catalog observation itself cannot run inside an aborted transaction.
    if (!tokens.empty()) {
        auto end=tokens.size()>1?tokens[1].start:query.size();
        auto first=query.substr(tokens[0].start,end-tokens[0].start);
        StringUtil::Trim(first);
        if (StringUtil::CIEquals(first,"ROLLBACK") || StringUtil::CIEquals(first,"COMMIT") || StringUtil::CIEquals(first,"BEGIN")) {return query;}
    }
    auto catalog=DatabaseManager::GetDefaultDatabase(*engine.connection.context);
    bool collision=false;
    engine.connection.context->RunFunctionInTransaction([&]() {
        auto &database=Catalog::GetCatalog(*engine.connection.context,catalog);
        collision=bool(database.GetSchema(*engine.connection.context,catalog,OnEntryNotFound::RETURN_NULL));
    });
    if (!collision) {return query;}
    vector<idx_t> insertions;
    for(idx_t index=0;index+2<tokens.size();++index) {
        if(tokens[index].type!=SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER || query[tokens[index+1].start]!='.') {continue;}
        if(index>0 && query[tokens[index-1].start]=='.') {continue;}
        if(index+3<tokens.size() && query[tokens[index+3].start]=='.') {continue;}
        auto name=query.substr(tokens[index].start,tokens[index+1].start-tokens[index].start);
        StringUtil::Trim(name);
        if(name.size()>1 && name.front()=='"' && name.back()=='"') {
            name=StringUtil::Replace(name.substr(1,name.size()-2),"\"\"","\"");
        }
        if(StringUtil::CIEquals(name,catalog)) {insertions.push_back(tokens[index].start);}
    }
    auto result=query;
    auto prefix=Quote(catalog)+".";
    for(auto entry=insertions.rbegin();entry!=insertions.rend();++entry) {result.insert(*entry,prefix);}
    return result;
}
}
extern "C" int grv_native_metadata_query(void *owner,const char *query,unsigned char *output,size_t allowance,size_t *written,char *error,size_t capacity) {
    try {
        auto &engine=*static_cast<Owner *>(owner)->engine;
        if (engine.snapshot_started || engine.result) {throw BinderException("metadata access cannot enter an extraction lifecycle");}
        MetadataScan inspection(engine,DatabaseManager::GetDefaultDatabase(*engine.connection.context));
        auto statements=engine.connection.ExtractStatements(QualifyCatalogCollision(engine,query));
        if (statements.size()!=1) {throw BinderException("internal metadata requires one statement");}
        auto result=engine.connection.Query(std::move(statements[0]));
        if (result->HasError()) {throw std::runtime_error(result->GetError());}
        if (result->ColumnCount()>16 || allowance<16 ||
            result->RowCount()>(allowance-16)/9/std::max<idx_t>(result->ColumnCount(),1)) {throw BinderException("metadata result exceeds bounded record allowance");}
        size_t offset=0;Put(output,offset,result->RowCount(),8);Put(output,offset,result->ColumnCount(),8);
        for (idx_t row=0;row<result->RowCount();++row) {
            for (idx_t column=0;column<result->ColumnCount();++column) {
                auto value=result->GetValue(column,row);
                auto text=value.IsNull()?string():value.ToString();
                if (allowance-offset<9 || text.size()>allowance-offset-9) {throw BinderException("metadata field exceeds bounded record allowance");}
                output[offset++]=value.IsNull()?0:1;Put(output,offset,text.size(),8);
                if (!text.empty()) {std::memcpy(output+offset,text.data(),text.size());offset+=text.size();}
            }
        }
        *written=offset;return 0;
    } catch (const std::exception &failure) {Error(error,capacity,failure);return -1;}
}
extern "C" int grv_native_managed_initialized(void *owner,int *initialized,char *error,size_t capacity) {
    try {
        auto &engine=*static_cast<Owner *>(owner)->engine;
        engine.connection.context->RunFunctionInTransaction([&]() {
            auto &catalog=Catalog::GetCatalog(*engine.connection.context,DatabaseManager::GetDefaultDatabase(*engine.connection.context));
            *initialized=catalog.GetSchema(*engine.connection.context,"_grv",OnEntryNotFound::RETURN_NULL)?1:0;
        });return 0;
    } catch (const std::exception &failure) {Error(error,capacity,failure);return -1;}
}
#include <fcntl.h>
namespace {
struct VerifiedFileScan {
    shared_ptr<GRVGuard> policy;
    DBConfig &configuration;
    idx_t previous_functions;
    unordered_set<string> previous_paths;
    std::shared_ptr<VerifiedDescriptorState> descriptors;
    VerifiedFileScan(Engine &engine,int descriptor) : policy(GRVGuard::Get(*engine.connection.context)),
        configuration(DBConfig::GetConfig(*engine.connection.context)),previous_functions(policy->tables.size()),previous_paths(configuration.options.allowed_paths),descriptors(engine.verified_descriptors) {
        struct stat metadata;
        auto flags = ::fcntl(descriptor,F_GETFL);
        if (descriptor<3 || flags<0 || (flags&O_ACCMODE)!=O_RDONLY || ::fstat(descriptor,&metadata)!=0 || !S_ISREG(metadata.st_mode)) {
            throw BinderException("verified scanner requires a pinned regular read-only descriptor");
        }
        std::lock_guard<std::mutex> guard(descriptors->mutex);
        if (descriptors->descriptor >= 0) { throw BinderException("verified descriptor scans cannot overlap"); }
        auto original = "/dev/fd/"+std::to_string(descriptor);
        if (descriptors->sequence == std::numeric_limits<uint64_t>::max()) { throw BinderException("verified descriptor identity exhausted"); }
        auto unique = "grv-verified-fd://"+std::to_string(++descriptors->sequence);
        int held = ::fcntl(descriptor,F_DUPFD_CLOEXEC,3);
        if (held < 0) { throw IOException("verified scanner could not retain its source descriptor"); }
        try {
            configuration.AddAllowedPath(original); configuration.AddAllowedPath(unique);
            policy->tables.insert(policy->tables.end(),policy->verified_file_scanners.begin(),policy->verified_file_scanners.end());
            descriptors->original = std::move(original); descriptors->unique = std::move(unique); descriptors->descriptor = held;
        } catch (...) {
            ::close(held); policy->tables.resize(previous_functions); configuration.options.allowed_paths=std::move(previous_paths); throw;
        }
    }
    ~VerifiedFileScan() {
        policy->tables.resize(previous_functions);configuration.options.allowed_paths=std::move(previous_paths);
        std::lock_guard<std::mutex> guard(descriptors->mutex);
        if (descriptors->descriptor >= 0) { ::close(descriptors->descriptor); descriptors->descriptor = -1; }
        descriptors->original.clear(); descriptors->unique.clear();
    }
};
}
extern "C" int grv_native_verified_file_query(void *owner,int descriptor,const char *query,unsigned char *output,size_t allowance,size_t *written,char *error,size_t capacity) {
    try {
        auto &engine=*static_cast<Owner *>(owner)->engine;
        VerifiedFileScan verified(engine,descriptor);
        return grv_native_metadata_query(owner,query,output,allowance,written,error,capacity);
    } catch (const std::exception &failure) {Error(error,capacity,failure);return -1;}
}
extern "C" int grv_native_relation_exists(void *owner,const char *schema,const char *table,int *exists,char *error,size_t capacity) {
    try {
        auto &engine=*static_cast<Owner *>(owner)->engine;
        engine.connection.context->RunFunctionInTransaction([&]() {
            auto &catalog=Catalog::GetCatalog(*engine.connection.context,DatabaseManager::GetDefaultDatabase(*engine.connection.context));
            auto source=catalog.GetSchema(*engine.connection.context,schema,OnEntryNotFound::RETURN_NULL);
            *exists=source && source->GetEntry(catalog.GetCatalogTransaction(*engine.connection.context),CatalogType::TABLE_ENTRY,table)?1:0;
        });return 0;
    } catch (const std::exception &failure) {Error(error,capacity,failure);return -1;}
}
// Catalog identity only: observing a relation never binds it or runs callbacks.
extern "C" int grv_native_base_table(void *owner,const char *schema,const char *table,int *native,char *error,size_t capacity) {
    try {auto &engine=*static_cast<Owner *>(owner)->engine;engine.connection.context->RunFunctionInTransaction([&]() {
        auto &catalog=Catalog::GetCatalog(*engine.connection.context,DatabaseManager::GetDefaultDatabase(*engine.connection.context));
        auto source=catalog.GetSchema(*engine.connection.context,schema,OnEntryNotFound::RETURN_NULL);
        auto entry=source?source->GetEntry(catalog.GetCatalogTransaction(*engine.connection.context),CatalogType::TABLE_ENTRY,table):nullptr;
        *native=entry && entry->type==CatalogType::TABLE_ENTRY && dynamic_cast<DuckTableEntry *>(entry.get())!=nullptr?1:0;
    });return 0;}catch(const std::exception &failure){Error(error,capacity,failure);return -1;}
}
extern "C" int grv_native_is_view(void *owner,const char *schema,const char *table,int *view,char *error,size_t capacity) {
    try {auto &engine=*static_cast<Owner *>(owner)->engine;engine.connection.context->RunFunctionInTransaction([&]() {
        auto &catalog=Catalog::GetCatalog(*engine.connection.context,DatabaseManager::GetDefaultDatabase(*engine.connection.context));
        auto source=catalog.GetSchema(*engine.connection.context,schema,OnEntryNotFound::RETURN_NULL);
        auto entry=source?source->GetEntry(catalog.GetCatalogTransaction(*engine.connection.context),CatalogType::TABLE_ENTRY,table):nullptr;
        *view=entry && entry->type==CatalogType::VIEW_ENTRY?1:0;
    });return 0;}catch(const std::exception &failure){Error(error,capacity,failure);return -1;}
}
extern "C" int grv_native_relation_schema(void *owner,const char *schema,const char *table,unsigned char *output,size_t allowance,size_t *written,char *error,size_t capacity) {
    try {auto &engine=*static_cast<Owner *>(owner)->engine;engine.connection.context->RunFunctionInTransaction([&]() {
        auto &catalog=Catalog::GetCatalog(*engine.connection.context,DatabaseManager::GetDefaultDatabase(*engine.connection.context));
        auto source=catalog.GetSchema(*engine.connection.context,schema,OnEntryNotFound::RETURN_NULL);
        auto entry=source?source->GetEntry(catalog.GetCatalogTransaction(*engine.connection.context),CatalogType::TABLE_ENTRY,table):nullptr;
        if(!entry) {throw CatalogException("relation schema unavailable");}
        vector<string> names; vector<LogicalType> types;
        if(entry->type==CatalogType::VIEW_ENTRY) {auto &view=entry->Cast<ViewCatalogEntry>(); auto columns=view.GetColumnInfo(); if(!columns) {throw CatalogException("view has no stored bound schema");} names=columns->names; types=columns->types;}
        else if(entry->type==CatalogType::TABLE_ENTRY) {for(auto &column:entry->Cast<TableCatalogEntry>().GetColumns().Logical()) {names.push_back(column.Name());types.push_back(column.Type());}}
        else {throw CatalogException("relation has no stored schema");}
        if(names.size()!=types.size() || names.size()>4096 || allowance<16) {throw BinderException("relation schema exceeds bounded metadata allowance");}
        size_t offset=0;Put(output,offset,names.size(),8);Put(output,offset,2,8);
        for(idx_t index=0;index<names.size();++index) {for(const auto &value:{names[index],types[index].ToString()}) {
            if(allowance-offset<9 || value.size()>allowance-offset-9) {throw BinderException("relation schema exceeds bounded metadata allowance");}
            output[offset++]=1;Put(output,offset,value.size(),8);std::memcpy(output+offset,value.data(),value.size());offset+=value.size();
        }} *written=offset;
    });return 0;}catch(const std::exception &failure){Error(error,capacity,failure);return -1;}
}
extern "C" int grv_native_schema_exists(void *owner,const char *schema,int *exists,char *error,size_t capacity) {
    try {auto &engine=*static_cast<Owner *>(owner)->engine;engine.connection.context->RunFunctionInTransaction([&]() {
        auto &catalog=Catalog::GetCatalog(*engine.connection.context,DatabaseManager::GetDefaultDatabase(*engine.connection.context));
        *exists=catalog.GetSchema(*engine.connection.context,schema,OnEntryNotFound::RETURN_NULL)?1:0;
    });return 0;}catch(const std::exception &failure){Error(error,capacity,failure);return -1;}
}
namespace {
// Exact local relation authorization exists only while evaluating user SELECTs.
// Nested view/macro lookup and automatic rebind still pass the native guard.
struct LocalQueryBindings {
    shared_ptr<GRVGuard> policy;
    std::set<string> relations;
    std::map<string,string> catalogs;
    LocalQueryBindings(Engine &engine) : policy(GRVGuard::Get(*engine.connection.context)),relations(policy->relations),catalogs(policy->relation_catalogs) {
        auto &context=*engine.connection.context;
        auto &catalog=Catalog::GetCatalog(context,DatabaseManager::GetDefaultDatabase(context));
        catalog.ScanSchemas(context,[&](SchemaCatalogEntry &schema) {
            if (std::set<string>{"_grv","grv_input","grv_self"}.count(StringUtil::Lower(schema.name)) || StringUtil::Lower(schema.name).find("_grv_session_")==0) {return;}
            for (auto type : {CatalogType::TABLE_ENTRY,CatalogType::MACRO_ENTRY,CatalogType::TABLE_MACRO_ENTRY}) {
                schema.Scan(context,type,[&](CatalogEntry &entry) {
                    if (entry.type!=CatalogType::TABLE_ENTRY && entry.type!=CatalogType::VIEW_ENTRY && entry.type!=CatalogType::MACRO_ENTRY && entry.type!=CatalogType::TABLE_MACRO_ENTRY) {return;}
                    auto name=StringUtil::Lower(schema.name+"."+entry.name);
                    policy->relations.insert(name);policy->relation_catalogs[name]=catalog.GetName();
                });
            }
        });
    }
    ~LocalQueryBindings() {policy->relations=std::move(relations);policy->relation_catalogs=std::move(catalogs);}
};
}
extern "C" int grv_native_stage_pull_query(void *owner,const char *query,const char *stage_name,const char *expected,char *error,size_t capacity) {
    try {
        auto &engine=*static_cast<Owner *>(owner)->engine;
        if (engine.snapshot_started || engine.result) {throw BinderException("pull selection cannot enter an extraction lifecycle");}
        string name(stage_name);
        if (name.find("_grv_pull_output_")!=0 || name.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789_")!=string::npos) {throw BinderException("invalid private pull stage");}
        auto statements=engine.connection.ExtractStatements(query);
        if (statements.size()!=1 || statements[0]->type!=StatementType::SELECT_STATEMENT) {throw BinderException("pull SQL requires exactly one read-only SELECT");}
        auto selected=QualifyCatalogCollision(engine,statements[0]->ToString());
        LocalQueryBindings scope(engine);
        auto required=engine.connection.Prepare(expected);
        auto prepared=engine.connection.Prepare(selected);
        if (required->HasError()) {throw BinderException(required->GetError());}
        if (prepared->HasError()) {throw BinderException(prepared->GetError());}
        if (prepared->GetTypes()!=required->GetTypes() || prepared->GetNames()!=required->GetNames()) {throw BinderException("pull SQL output differs from exact ordered output contract");}
        CheckedQuery(engine,"CREATE TEMP TABLE "+Quote(name)+" AS "+selected);
        auto policy=GRVGuard::Get(*engine.connection.context);
        policy->private_relations.insert("main."+name);
        return 0;
    }catch(const std::exception &failure){Error(error,capacity,failure);return -1;}
}
// This seam is available only to closed generated destination commands, never
// to declaration SQL. It permits reading private outputs after all queries stage.
extern "C" int grv_native_private_command(void *owner,const char *query,unsigned char *output,size_t allowance,size_t *written,char *error,size_t capacity) {
    try {auto &engine=*static_cast<Owner *>(owner)->engine;InternalScan scope(engine);return grv_native_metadata_query(owner,query,output,allowance,written,error,capacity);}
    catch(const std::exception &failure){Error(error,capacity,failure);return -1;}
}

// Managed output SQL has a narrower dependency scope than application pull SQL.
// Only actual prepared input/self tables and pure local macros are reachable.
namespace {
struct BuildQueryBindings {
    shared_ptr<GRVGuard> policy;
    std::set<string> previous;
    std::map<string,string> catalogs;
    BuildQueryBindings(Engine &engine) : policy(GRVGuard::Get(*engine.connection.context)),previous(policy->relations),catalogs(policy->relation_catalogs) {
        policy->relations.clear();policy->relation_catalogs.clear();
        auto &context=*engine.connection.context;
        auto &catalog=Catalog::GetCatalog(context,DatabaseManager::GetDefaultDatabase(context));
        catalog.ScanSchemas(context,[&](SchemaCatalogEntry &schema) {
            bool input=StringUtil::CIEquals(schema.name,"grv_input") || StringUtil::CIEquals(schema.name,"grv_self");
            if (input) {schema.Scan(context,CatalogType::TABLE_ENTRY,[&](CatalogEntry &entry) {
                if(entry.type!=CatalogType::TABLE_ENTRY){return;}
                auto name=StringUtil::Lower(schema.name+"."+entry.name);policy->relations.insert(name);policy->relation_catalogs[name]=catalog.GetName();
            });}
            if (StringUtil::CIEquals(schema.name,"_grv") || StringUtil::Lower(schema.name).find("_grv_session_")==0) {return;}
            for(auto type:{CatalogType::MACRO_ENTRY,CatalogType::TABLE_MACRO_ENTRY}) {schema.Scan(context,type,[&](CatalogEntry &entry) {
                if(entry.type!=CatalogType::MACRO_ENTRY && entry.type!=CatalogType::TABLE_MACRO_ENTRY){return;}
                auto name=StringUtil::Lower(schema.name+"."+entry.name);policy->relations.insert(name);policy->relation_catalogs[name]=catalog.GetName();
            });}
        });
    }
    ~BuildQueryBindings(){policy->relations=std::move(previous);policy->relation_catalogs=std::move(catalogs);}
};
}
extern "C" int grv_native_stage_build_query(void *owner,const char *query,const char *projection,const char *stage_name,const char *expected,unsigned char *schema_output,size_t schema_allowance,size_t *schema_written,char *error,size_t capacity) {
    try {
        auto &engine=*static_cast<Owner *>(owner)->engine;
        if(engine.snapshot_started || engine.result){throw BinderException("build evaluation cannot enter an extraction lifecycle");}
        string name(stage_name);
        if(name.find("_grv_build_output_")!=0 || name.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789_")!=string::npos){throw BinderException("invalid private build stage");}
        auto statements=engine.connection.ExtractStatements(query);
        if(statements.size()!=1 || statements[0]->type!=StatementType::SELECT_STATEMENT){throw BinderException("build SQL requires exactly one read-only SELECT");}
        // Serialize exactly one parsed SELECT before embedding its projection;
        // file-ending semicolons/comments never escape or invalidate the wrapper.
        auto selected=QualifyCatalogCollision(engine,"SELECT "+string(projection)+" FROM ("+statements[0]->ToString()+") AS "+Quote("_grv_build_projection"));
        BuildQueryBindings scope(engine);
        auto required=engine.connection.Prepare(expected);auto prepared=engine.connection.Prepare(selected);
        if(required->HasError()){throw BinderException(required->GetError());}
        if(prepared->HasError()){throw BinderException(prepared->GetError());}
        if(prepared->GetNames()!=required->GetNames()){throw BinderException("build SQL output differs from exact ordered column mappings");}
        auto types=prepared->GetTypes();
        if(types.empty() || schema_allowance<4+6*types.size()){throw BinderException("build schema exceeds bounded metadata allowance");}
        size_t offset=0;Put(schema_output,offset,types.size(),4);
        for(const auto &type:types) {
            Put(schema_output,offset,TypeCode(type),4);
            Put(schema_output,offset,type.id()==LogicalTypeId::DECIMAL?DecimalType::GetWidth(type):0,1);
            Put(schema_output,offset,type.id()==LogicalTypeId::DECIMAL?DecimalType::GetScale(type):0,1);
        }
        *schema_written=offset;
        CheckedQuery(engine,"CREATE TEMP TABLE "+Quote(name)+" AS "+selected);
        GRVGuard::Get(*engine.connection.context)->private_relations.insert("main."+name);return 0;
    }catch(const std::exception &failure){Error(error,capacity,failure);return -1;}
}
// Closing invocation connections is an explicit lifetime barrier. The same
// independent interrupt control and Rust workspace ownership survive it.
extern "C" int grv_native_close_invocation(void *owner,const char *path,char *error,size_t capacity) {
    auto *value=static_cast<Owner *>(owner);
    auto state=value->interrupt;std::lock_guard<std::mutex> guard(state->mutex);
    state->engine=nullptr;value->engine.reset();
    auto reopened=static_cast<Owner *>(grv_native_open_mode(path,false,error,capacity));
    if(!reopened){return -1;}
    value->engine=std::move(reopened->engine);state->engine=value->engine.get();delete reopened;return 0;
}
