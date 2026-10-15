// Actual setup exception/secret-revocation slices with synthetic canaries.
#define GRV_GCS_SETUP_PROOF
#include "shim.cpp"
#include "duckdb/main/settings.hpp"
#include <iostream>
bool PrivateSecret(Engine &engine){
    bool present=false;
    engine.connection.context->RunFunctionInTransaction([&](){
        auto transaction=CatalogTransaction::GetSystemCatalogTransaction(*engine.connection.context);
        present=bool(SecretManager::Get(*engine.connection.context).GetSecretByName(transaction,"_grv_exact_gcs_reader","memory"));
    });return present;
}
int main(int argc,char **argv){
    if(argc!=2)return 2;
    const char *canary="synthetic-setup-canary-not-a-credential";
    try {
        for(int stage=1;stage<=3;++stage){
            char error[4096]={0};void *owner=grv_native_open(nullptr,error,sizeof(error));if(!owner)throw IOException("owner");
            auto &engine=*static_cast<Owner *>(owner)->engine;
            auto policy=GRVGuard::Get(*engine.connection.context);auto paths=DBConfig::GetConfig(*engine.connection.context).options.allowed_paths;
            gcs_setup_failure_stage=stage;gcs_setup_drop_failure=false;
            int status=grv_native_gcs_configure(owner,argv[1],"https://storage.googleapis.com/test-bucket/data",canary,"named-project",10000,error,sizeof(error));
            if(status!=-1||PrivateSecret(engine)||!engine.exact_gcs->failed||string(error).find(canary)!=string::npos)throw IOException("cleanup");
            auto &config=DBConfig::GetConfig(*engine.connection.context);
            if(Settings::Get<EnableExternalAccessSetting>(config)||config.options.allowed_paths!=paths||GRVGuard::Get(*engine.connection.context)!=policy)throw IOException("guard restoration");
            unsigned char output[4096];size_t written=0;
            const char *urls[]={"https://storage.googleapis.com/test-bucket/data/proof.parquet"};
            if(!grv_native_gcs_query(owner,urls,1,"SELECT * FROM read_parquet('https://storage.googleapis.com/test-bucket/data/proof.parquet')",output,sizeof(output),&written,error,sizeof(error)))throw IOException("failed setup read authority");
            if(!grv_native_metadata_query(owner,"SELECT * FROM duckdb_secrets()",output,sizeof(output),&written,error,sizeof(error)))throw IOException("secret introspection");
            if(grv_native_metadata_query(owner,"BEGIN",output,sizeof(output),&written,error,sizeof(error))||grv_native_metadata_query(owner,"ROLLBACK",output,sizeof(output),&written,error,sizeof(error)))throw IOException("local rollback");
            // A second configure on a failed owner is refused with proved
            // cleanup (the secret is already gone); it admits no new authority.
            int again=grv_native_gcs_configure(owner,argv[1],"https://storage.googleapis.com/test-bucket/data",canary,"named-project",10000,error,sizeof(error));
            if(again!=-1||PrivateSecret(engine)||string(error).find(canary)!=string::npos)throw IOException("reconfigure of a failed owner");
            if(!engine.exact_gcs->admitted.empty()||engine.exact_gcs->active)throw IOException("failed owner admitted new authority");
            grv_native_close(owner);
        }
        char error[4096]={0};void *owner=grv_native_open(nullptr,error,sizeof(error));if(!owner)throw IOException("owner");
        gcs_setup_failure_stage=1;gcs_setup_drop_failure=true;
        auto &engine=*static_cast<Owner *>(owner)->engine;
        int status=grv_native_gcs_configure(owner,argv[1],"https://storage.googleapis.com/test-bucket/data",canary,"named-project",10000,error,sizeof(error));
        if(status!=-2||!PrivateSecret(engine)||!engine.exact_gcs->failed||string(error).find(canary)!=string::npos)throw IOException("unproved revocation not distinguished");
        // The test destroys the owner to model the in-process recovery
        // contract: an unproved (-2) setup quarantines the owner; recovery is
        // a synchronous owner-thread close and a fresh reopen preserving the
        // original access mode under the retained workspace lock (Rust level). The in-process
        // architecture has no dedicated helper process to exit and no process
        // fence to prove. Never reuse a retained token.
        unsigned char output[4096];size_t written=0;
        const char *urls[]={"https://storage.googleapis.com/test-bucket/data/proof.parquet"};
        if(!grv_native_gcs_query(owner,urls,1,"SELECT 1",output,sizeof(output),&written,error,sizeof(error)))throw IOException("unproved cleanup read authority");
        grv_native_close(owner);
        for(int scenario=0;scenario<2;++scenario){
            char private_error[4096]={0};void *private_owner=grv_native_open(nullptr,private_error,sizeof(private_error));if(!private_owner)throw IOException("owner");
            gcs_setup_drop_failure=false;gcs_setup_inspection_failure=scenario==0;gcs_setup_preamble_failure=scenario==1;
            auto &private_engine=*static_cast<Owner *>(private_owner)->engine;
            int refused=grv_native_gcs_configure(private_owner,argv[1],"https://storage.googleapis.com/test-bucket/data",canary,"named-project",10000,private_error,sizeof(private_error));
            if(refused!=-2||string(private_error).find(canary)!=string::npos)throw IOException("uncertain completion/preamble escaped");
            // Exception after observing absence must invalidate that evidence.
            if(scenario==0&&(PrivateSecret(private_engine)||!private_engine.exact_gcs->failed))throw IOException("inspection exception classification");
            if(!grv_native_gcs_query(private_owner,urls,1,"SELECT 1",output,sizeof(output),&written,private_error,sizeof(private_error)))throw IOException("uncertain setup admission");
            grv_native_close(private_owner);
        }
        gcs_setup_inspection_failure=false;gcs_setup_preamble_failure=false;
        std::cout<<"post-secret setup faults and unproved-revocation status passed\n";return 0;
    }catch(...){std::cerr<<"GCS setup proof failed (fixed private diagnostic)\n";return 1;}
}
