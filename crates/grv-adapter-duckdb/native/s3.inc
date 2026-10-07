namespace {
string S3Literal(const string &value) {return "'"+StringUtil::Replace(value,"'","''")+"'";}
void S3Trusted(Engine &engine,const string &query) {auto result=engine.connection.Query(query);if(result->HasError()) {throw IOException("trusted S3 reader setup failed");}}
struct VerifiedS3Scan {
    Engine &engine; shared_ptr<GRVGuard> policy;idx_t previous_functions;unordered_set<string> paths;std::set<string> active;
    VerifiedS3Scan(Engine &engine,const vector<string> &uris):engine(engine),policy(GRVGuard::Get(*engine.connection.context)),previous_functions(policy->tables.size()),paths(DBConfig::GetConfig(*engine.connection.context).options.allowed_paths),active(engine.verified_s3->active) {
        try {for(auto &uri:uris) {auto object=engine.verified_s3->objects.at(uri);DBConfig::GetConfig(*engine.connection.context).AddAllowedPath("s3://"+object->bucket+"/"+object->key);engine.verified_s3->active.insert(uri);}policy->tables.insert(policy->tables.end(),policy->verified_file_scanners.begin(),policy->verified_file_scanners.end());}
        catch(...) {policy->tables.resize(previous_functions);DBConfig::GetConfig(*engine.connection.context).options.allowed_paths=paths;engine.verified_s3->active=active;throw;}
    }
    ~VerifiedS3Scan(){policy->tables.resize(previous_functions);DBConfig::GetConfig(*engine.connection.context).options.allowed_paths=std::move(paths);engine.verified_s3->active=std::move(active);}
};
}
extern "C" int grv_native_s3_configure(void *owner,const char *httpfs,const char *aws,const char *scope,const char *profile,const char *region,char *error,size_t capacity) {
    auto &engine=*static_cast<Owner *>(owner)->engine;auto &configuration=DBConfig::GetConfig(*engine.connection.context);auto policy=GRVGuard::Get(*engine.connection.context);
    auto paths=configuration.options.allowed_paths;int phase=0;
    try {
        if(engine.snapshot_started||engine.result||engine.verified_s3->configured) {throw PermissionException("S3 reader configuration is fixed per engine lifetime");}
        string fixed_scope(scope),fixed_region(region),fixed_profile(profile);
        auto coordinate=S3Parse(fixed_scope+"/grv-scope-proof");string decoded_scope="s3://"+coordinate.bucket;auto prefix=coordinate.key=="grv-scope-proof"?string():coordinate.key.substr(0,coordinate.key.size()-std::strlen("/grv-scope-proof"));if(!prefix.empty()) {decoded_scope+="/"+prefix;}if(fixed_profile.empty()||fixed_profile.size()>1024||fixed_region.empty()||fixed_region.size()>128||fixed_profile.find_first_of("\r\n")!=string::npos||fixed_region.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-")!=string::npos) {throw PermissionException("invalid S3 reader configuration");}
        // No untrusted query can enter this owner while its guard is temporarily
        // absent. Paths were hash-verified and copied into protected immutable files.
        phase=1;engine.connection.context->registered_state->Remove("grv.native.guard.v1");
        configuration.SetOptionByName("enable_external_access",Value(true));configuration.AddAllowedPath(httpfs);configuration.AddAllowedPath(aws);
        phase=2;SecretManager::Get(*engine.connection.context).SetEnablePersistentSecrets(false);
        phase=3;S3Trusted(engine,"SET enable_logging=false");
        phase=4;S3Trusted(engine,"LOAD "+S3Literal(httpfs));phase=5;S3Trusted(engine,"LOAD "+S3Literal(aws));
        phase=6;S3Trusted(engine,"SET s3_url_compatibility_mode=true");S3Trusted(engine,"SET enable_http_metadata_cache=false");
        S3Trusted(engine,"SET enable_external_file_cache=false");
        // An explicit PROFILE without CHAIN uses AWS's named file provider,
        // never a default environment/instance credential chain.
        const string name="_grv_exact_s3_reader";
        phase=7;S3Trusted(engine,"CREATE SECRET "+Quote(name)+" (TYPE s3,PROVIDER credential_chain,PROFILE "+S3Literal(fixed_profile)+",REGION "+S3Literal(fixed_region)+",SCOPE "+S3Literal(decoded_scope)+",URL_COMPATIBILITY_MODE true)");
        phase=8;engine.connection.context->RunFunctionInTransaction([&]() {
            auto &manager=SecretManager::Get(*engine.connection.context);
            auto transaction=Catalog::GetSystemCatalog(*engine.connection.context).GetCatalogTransaction(*engine.connection.context);
            auto entry=manager.GetSecretByName(transaction,name,SecretManager::TEMPORARY_STORAGE_NAME);
            if(!entry || !entry->secret) {throw PermissionException("named independent S3 credentials unavailable");}
            auto &secret=dynamic_cast<const KeyValueSecret &>(*entry->secret);
            auto read=[&](const string &key){auto value=secret.TryGetValue(key);return value.IsNull()?string():value.ToString();};
            engine.verified_s3->key_id=read("key_id");engine.verified_s3->secret=read("secret");engine.verified_s3->token=read("session_token");
            if(engine.verified_s3->key_id.empty()||engine.verified_s3->secret.empty()||engine.verified_s3->key_id.size()>4096||engine.verified_s3->secret.size()>4096||engine.verified_s3->token.size()>16384) {throw PermissionException("named independent S3 credentials unavailable");}
            policy=Policy(*engine.connection.context);
        });
        engine.verified_s3->scope=fixed_scope;engine.verified_s3->region=fixed_region;engine.verified_s3->configured=true;
        configuration.SetOptionByName("enable_external_access",Value(false));configuration.options.allowed_paths=std::move(paths);
        engine.connection.context->registered_state->Insert("grv.native.guard.v1",policy);return 0;
    } catch (...) {
        configuration.SetOptionByName("enable_external_access",Value(false));configuration.options.allowed_paths=std::move(paths);
        engine.connection.context->registered_state->Remove("grv.native.guard.v1");engine.connection.context->registered_state->Insert("grv.native.guard.v1",policy);
        if(capacity) {auto message="independent S3 reader initialization failed at protected setup phase "+std::to_string(phase);std::strncpy(error,message.c_str(),capacity-1);error[capacity-1]=0;}return -1;
    }
}
extern "C" int grv_native_s3_query(void *owner,const char *const *locations,const uint64_t *sizes,const char *const *hashes,const char *const *validators,size_t count,const char *query,unsigned char *output,size_t allowance,size_t *written,char *error,size_t capacity) {
    try {
        auto &engine=*static_cast<Owner *>(owner)->engine;
        if(count==0||count>4096||engine.snapshot_started||engine.result) {throw PermissionException("invalid exact S3 scanner lifecycle");}
        vector<string> uris;std::set<string> unique;size_t metadata=0;
        for(size_t index=0;index<count;++index) {string uri(locations[index]);metadata+=uri.size()+std::strlen(hashes[index])+std::strlen(validators[index])+128;if(metadata>2*1024*1024||!unique.insert(uri).second) {throw PermissionException("exact S3 file set exceeds bounded metadata allowance");}engine.verified_s3->Verify(uri,sizes[index],hashes[index],validators[index]);uris.push_back(std::move(uri));}
        VerifiedS3Scan scope(engine,uris);engine.verified_s3->stage=50;engine.verified_s3->status=0;
        if(grv_native_metadata_query(owner,query,output,allowance,written,error,capacity)!=0) {throw IOException("verified S3 scanner query refused");}return 0;
    } catch (...) {if(capacity) {auto state=static_cast<Owner *>(owner)->engine->verified_s3;auto message="exact verified S3 scan failed at protected read phase "+std::to_string(state->stage)+" (HTTP status "+std::to_string(state->status)+")";std::strncpy(error,message.c_str(),capacity-1);error[capacity-1]=0;}return -1;}
}
// Pure conformance seam: proves GRV coordinates and SigV4 path encoding are
// distinct, without credential acquisition, catalog mutation or HTTP calls.
extern "C" int grv_native_s3_uri_roundtrip(const char *uri,char *decoded,size_t allowance) {
    try {auto coordinate=S3Parse(uri);auto path="/"+coordinate.bucket+"/"+S3Encode(coordinate.key,true);if(path.size()+1>allowance) {return -1;}std::memcpy(decoded,path.c_str(),path.size()+1);return 0;}catch(...) {return -1;}
}
