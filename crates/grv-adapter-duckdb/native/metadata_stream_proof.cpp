// Source-including native metadata/cleanup proof. No transport or credentials.
#include "shim.cpp"
#include <iostream>
struct LateEndFailure : ClientContextState {
    void QueryEnd(ClientContext &,optional_ptr<ErrorData>) override {throw IOException("synthetic query end failure");}
};
int main(){
    char error[4096]={0};void *owner=grv_native_open(nullptr,error,sizeof(error));if(!owner)return 1;
    try {
        auto &engine=*static_cast<Owner *>(owner)->engine;
        // Test-only state activation exercises actual GCS metadata branch.
        // No reader transport/secret is installed or network made admissible.
        engine.exact_gcs->configured=true;
        vector<unsigned char> bytes(65536);size_t written=0;
        auto query=[&](const char *sql){written=0;return grv_native_metadata_query(owner,sql,bytes.data(),bytes.size(),&written,error,sizeof(error));};
        if(query("SELECT 42::BIGINT")||written!=27)throw IOException("scalar");
        if(query("BEGIN")||query("CREATE TABLE _grv_stream_proof(id BIGINT)"))throw IOException("transaction");
        if(!query("SELECT i FROM range(1000000000) r(i)"))throw IOException("row cap");
        if(engine.exact_gcs->failed)throw IOException("ordinary output refusal revoked authority");
        if(query("ROLLBACK")||query("SELECT 42::BIGINT"))throw IOException("cleanup rollback");
        if(engine.connection.context->registered_state->Get<GCSMetadataEndState>("grv.gcs.metadata.end"))throw IOException("listener leak");
        engine.connection.context->registered_state->Insert("proof.lateend",make_shared_ptr<LateEndFailure>());
        if(!query("SELECT i FROM range(1000000000) r(i)"))throw IOException("injected cleanup failure accepted");
        if(!engine.exact_gcs->failed)throw IOException("cleanup failure did not revoke authority");
        engine.connection.context->registered_state->Remove("proof.lateend");
        if(engine.connection.context->registered_state->Get<GCSMetadataEndState>("grv.gcs.metadata.end"))throw IOException("failed listener leak");
        grv_native_close(owner);std::cout<<"bounded metadata cleanup and injected QueryEnd revocation passed\n";return 0;
    } catch(...) {grv_native_close(owner);std::cerr<<"metadata stream proof failed (fixed private diagnostic)\n";return 1;}
}
