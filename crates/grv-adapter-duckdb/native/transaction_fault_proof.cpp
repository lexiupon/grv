// Source-including real WAL I/O failure proof; no credentials or network.
#include "shim.cpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include <iostream>
struct WALFaultState {bool sync=false,write=false;size_t hits=0;};
struct WALFaultFileSystem : LocalFileSystem {
    std::shared_ptr<WALFaultState> state;
    explicit WALFaultFileSystem(std::shared_ptr<WALFaultState> state):state(std::move(state)){}
    bool WAL(FileHandle &handle){return StringUtil::EndsWith(handle.path,".wal");}
    void FileSync(FileHandle &handle) override {
        if(state->sync&&WAL(handle)){++state->hits;throw IOException("synthetic WAL sync refusal");}
        LocalFileSystem::FileSync(handle);
    }
    int64_t Write(FileHandle &handle,void *bytes,int64_t count) override {
        if(state->write&&WAL(handle)){++state->hits;throw IOException("synthetic WAL write refusal");}
        return LocalFileSystem::Write(handle,bytes,count);
    }
    void Write(FileHandle &handle,void *bytes,int64_t count,idx_t position) override {
        if(state->write&&WAL(handle)){++state->hits;throw IOException("synthetic WAL write refusal");}
        LocalFileSystem::Write(handle,bytes,count,position);
    }
};
std::unique_ptr<Owner> FaultOwner(const char *path,std::shared_ptr<WALFaultState> state){
    DBConfig config;
    config.file_system=make_uniq<VirtualFileSystem>(make_uniq<WALFaultFileSystem>(std::move(state)));
    config.SetOptionByName("threads",Value(int64_t(1)));
    config.SetOptionByName("enable_external_access",Value(false));
    config.SetOptionByName("autoload_known_extensions",Value(false));
    config.SetOptionByName("autoinstall_known_extensions",Value(false));
    auto engine=std::make_unique<Engine>(path,config);
    engine->database.LoadStaticExtension<ParquetExtension>();
    engine->connection.context->config.use_replacement_scans=false;
    shared_ptr<GRVGuard> policy;
    engine->connection.context->RunFunctionInTransaction([&](){policy=Policy(*engine->connection.context);});
    engine->connection.context->registered_state->Insert("grv.native.guard.v1",policy);
    auto interrupt=std::make_shared<GRVInterruptControl>();interrupt->engine=engine.get();
    return std::make_unique<Owner>(Owner{std::move(engine),std::move(interrupt)});
}
struct ObserveRollbackError : ClientContextState {
    string message;
    void QueryEnd(ClientContext &,optional_ptr<ErrorData> error) override {
        if(error&&error->HasError())message=error->RawMessage();
    }
};
struct DisposableDirectory {
    string path;
    DisposableDirectory(){char pattern[]="/tmp/grv-transaction-fault-XXXXXX";auto result=::mkdtemp(pattern);if(!result)throw IOException("private directory");path=result;}
    ~DisposableDirectory(){try{FileSystem::CreateLocal()->RemoveDirectory(path);}catch(...) {}}
};
int main(){
    try {
        DisposableDirectory directory;
        for(int fault=0;fault<2;++fault){
            auto path=directory.path+"/proof-"+std::to_string(fault)+".duckdb";
            auto state=std::make_shared<WALFaultState>();
            auto owner=FaultOwner(path.c_str(),state);auto &engine=*owner->engine;
            // Test-only configured branch; never install a reader/secret.
            engine.exact_gcs->configured=true;
            unsigned char bytes[65536];size_t written=0;char error[4096]={0};
            auto query=[&](const char *sql){written=0;return grv_native_metadata_query(owner.get(),sql,bytes,sizeof(bytes),&written,error,sizeof(error));};
            if(query("CREATE TABLE _grv_fault_history(id BIGINT)")||query("INSERT INTO _grv_fault_history VALUES (42)"))throw IOException("baseline");
            if(query("BEGIN")||query("INSERT INTO _grv_fault_history VALUES (99)"))throw IOException("stage");
            state->sync=fault==0;state->write=fault==1;
            if(!query("COMMIT")||!state->hits)throw IOException("actual commit failure missing");
            if(!engine.exact_gcs->failed)throw IOException("commit failure authority retained");
            if(engine.connection.context->registered_state->Get<GCSMetadataEndState>("grv.gcs.metadata.end"))throw IOException("witness retained");
            // Observe committed visibility before orderly shutdown can
            // checkpoint anything, with fault authority still armed. This
            // independent reader never repeats the attempted mutation.
            {
                Connection independent(engine.database);
                auto visible=independent.Query("SELECT id FROM _grv_fault_history ORDER BY id");
                if(visible->HasError()||visible->RowCount()!=1||visible->GetValue(0,0).GetValue<int64_t>()!=42)throw IOException("pre-close committed history");
            }
            // No retry of COMMIT/INSERT. Dispose this owner and resolve durable
            // history on a fresh source-free engine with original faults off.
            state->sync=false;state->write=false;owner.reset();
            DuckDB reopened(path.c_str());Connection recovery(reopened);
            auto result=recovery.Query("SELECT id FROM _grv_fault_history ORDER BY id");
            if(result->HasError()||result->RowCount()!=1||result->GetValue(0,0).GetValue<int64_t>()!=42)throw IOException("immutable outcome recovery");
        }
        // Actual MetaTransaction::Rollback refuses an invalidated attached
        // database; inject invalidation while an autocommit stream is active,
        // before its interrupted cleanup (not a throwing QueryEnd listener).
        {
            auto path=directory.path+"/rollback.duckdb";auto state=std::make_shared<WALFaultState>();
            auto owner=FaultOwner(path.c_str(),state);auto &engine=*owner->engine;
            engine.exact_gcs->configured=true;
            auto &context=*engine.connection.context;
            shared_ptr<GCSMetadataEndState> observed;
            auto provenance=make_shared_ptr<ObserveRollbackError>();
            context.registered_state->Insert("proof.rollback.provenance",provenance);
            {
                GCSMetadataBufferLimit buffer(context);
                GCSMetadataEnd end(context,*engine.exact_gcs);observed=end.state;
                auto statements=engine.connection.ExtractStatements("SELECT i FROM range(1000000000) r(i)");
                GCSMetadataResult result(engine.connection.SendQuery(std::move(statements[0])),end.state,*engine.exact_gcs);
                if(result.result->HasError()||!result.result->FetchRaw())throw IOException("active rollback stream");
                auto &catalog=Catalog::GetCatalog(context,DatabaseManager::GetDefaultDatabase(context));
                ValidChecker::Invalidate(catalog.GetAttached(),"synthetic attached database invalidation before rollback");
            }
            if(!observed->ended||!observed->cleanup_error||!engine.exact_gcs->failed||context.transaction.HasActiveTransaction())throw IOException("actual rollback error not fenced");
            if(context.registered_state->Get<GCSMetadataEndState>("grv.gcs.metadata.end"))throw IOException("rollback witness retained");
            if(provenance->message.find("synthetic attached database invalidation before rollback")==string::npos)throw IOException("actual rollback error provenance absent");
            context.registered_state->Remove("proof.rollback.provenance");
            owner.reset();
            DuckDB reopened(path.c_str());Connection recovery(reopened);
            auto result=recovery.Query("SELECT 42");if(result->HasError())throw IOException("source-free invalidation reopen");
        }
        std::cout<<"actual WAL commit refusal and invalidated-database rollback cleanup revoke authority and reopen\n";return 0;
    }catch(...){std::cerr<<"transaction fault proof failed (fixed private diagnostic)\n";return 1;}
}
