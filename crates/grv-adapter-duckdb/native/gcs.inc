namespace {
struct GCSPrivateString {
    string value;
    explicit GCSPrivateString(const char *input):value(input){}
    ~GCSPrivateString(){volatile char *data=value.empty()?nullptr:&value[0];for(size_t i=0;i<value.size();++i)data[i]=0;}
};
void GCSTrusted(Engine &engine,const string &query) {
    auto result=engine.connection.Query(query);
    if(result->HasError())throw IOException("trusted GCS setup failed");
}
struct GCSScanScope {
    Engine &engine;shared_ptr<GRVGuard> policy;idx_t functions;
    unordered_set<string> paths;
    GCSScanScope(Engine &engine,const vector<string> &urls):engine(engine),policy(GRVGuard::Get(*engine.connection.context)),functions(policy->tables.size()),paths(DBConfig::GetConfig(*engine.connection.context).options.allowed_paths) {
        auto &state=*engine.exact_gcs;
        try {
            {std::lock_guard<std::mutex> lock(state.mutex);
                if(!state.configured||state.failed||state.active||std::chrono::steady_clock::now()>=state.deadline)throw PermissionException("GCS scanner lease inactive");
                std::set<string> admitted;
                size_t metadata=0;
                for(const auto &url:urls){GCSValidate(url);metadata+=url.size()+128;
                    if(metadata>2*1024*1024||!admitted.insert(url).second||!(url==state.scope||StringUtil::StartsWith(url,state.scope+"/")))throw PermissionException("invalid exact GCS file admission");}
                state.admitted=std::move(admitted);state.active=true;++state.epoch;
            }
            auto &config=DBConfig::GetConfig(*engine.connection.context);
            for(const auto &url:urls)config.AddAllowedPath(url);
            policy->tables.insert(policy->tables.end(),policy->verified_file_scanners.begin(),policy->verified_file_scanners.end());
        } catch(...) {policy->tables.resize(functions);DBConfig::GetConfig(*engine.connection.context).options.allowed_paths=paths;std::lock_guard<std::mutex> lock(state.mutex);state.active=false;state.admitted.clear();throw;}
    }
    ~GCSScanScope(){policy->tables.resize(functions);DBConfig::GetConfig(*engine.connection.context).options.allowed_paths=std::move(paths);std::lock_guard<std::mutex> lock(engine.exact_gcs->mutex);engine.exact_gcs->active=false;engine.exact_gcs->admitted.clear();}
};
}
extern "C" int grv_native_gcs_configure(void *owner,const char *httpfs,const char *scope,const char *bearer,const char *project,uint64_t remaining_millis,char *error,size_t capacity) {
    auto &engine=*static_cast<Owner *>(owner)->engine;auto &config=DBConfig::GetConfig(*engine.connection.context);auto policy=GRVGuard::Get(*engine.connection.context);auto paths=config.options.allowed_paths;
    try {
        auto &state=*engine.exact_gcs;
        if(engine.snapshot_started||engine.result||engine.verified_s3->configured||state.configured||state.failed||remaining_millis==0||remaining_millis>300000)throw PermissionException("invalid GCS reader lifecycle");
        string fixed_scope(scope),fixed_project(project);GCSPrivateString private_token(bearer);auto &token=private_token.value;GCSValidate(fixed_scope,true);
        if(token.empty()||token.size()>16384||token.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~+/=")!=string::npos||fixed_project.empty()||fixed_project.size()>128||fixed_project.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-")!=string::npos)throw PermissionException("invalid private GCS setup");
        state.deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(remaining_millis);
        engine.connection.context->registered_state->Remove("grv.native.guard.v1");
        config.SetOptionByName("enable_external_access",Value(true));config.AddAllowedPath(httpfs);
        SecretManager::Get(*engine.connection.context).SetEnablePersistentSecrets(false);
        GCSTrusted(engine,"SET enable_logging=false");GCSTrusted(engine,"LOAD "+S3Literal(httpfs));
        // Pinned curl's timeout is low-speed/connect only (no hard transfer
        // deadline and ignores subsecond fields). Only signed stock httplib
        // is admitted for bounded GCS executions; no new transport is introduced.
        GCSTrusted(engine,"SET httpfs_client_implementation='httplib'");
        GCSTrusted(engine,"SET enable_http_metadata_cache=false");GCSTrusted(engine,"SET enable_external_file_cache=false");
        GCSTrusted(engine,"SET http_retries=0");GCSTrusted(engine,"SET http_timeout=30");
        GCSTrusted(engine,"SET force_download=false");GCSTrusted(engine,"SET auto_fallback_to_full_download=false");
        GCSTrusted(engine,"CREATE TEMPORARY SECRET _grv_exact_gcs_reader (TYPE http,BEARER_TOKEN "+S3Literal(token)+",SCOPE "+S3Literal(fixed_scope)+",VERIFY_SSL true)");
        std::fill(token.begin(),token.end(),'\0');
        auto &stock=config.GetHTTPUtil();
        std::weak_ptr<ExactGCSState> weak=engine.exact_gcs;
        config.SetHTTPUtil(make_shared_ptr<grv::GCSHTTPPolicy>(stock,fixed_project,[weak](BaseRequest &request,unique_ptr<HTTPClient> &client){auto state=weak.lock();if(!state)throw PermissionException("GCS owner no longer exists");state->BeforeRequest(request,client);}));
        auto &fs=FileSystem::GetFileSystem(*engine.connection.context);auto original=fs.ExtractSubSystem("HTTPFileSystem");
        if(!original)throw IOException("signed stock HTTPFS filesystem unavailable");
        fs.RegisterSubSystem(make_uniq<ExactGCSFileSystem>(engine.exact_gcs,std::move(original)));
        engine.connection.context->RunFunctionInTransaction([&](){policy=Policy(*engine.connection.context);});
        state.scope=fixed_scope;state.configured=true;
        config.SetOptionByName("enable_external_access",Value(false));config.options.allowed_paths=std::move(paths);
        engine.connection.context->registered_state->Insert("grv.native.guard.v1",policy);return 0;
    } catch(...) {
        engine.exact_gcs->failed=true;
        // Setup can fail after installing the private temporary secret. Revoke
        // it before restoring the user guard; never retain failed credentials.
        engine.connection.context->registered_state->Remove("grv.native.guard.v1");
        try {GCSTrusted(engine,"DROP SECRET IF EXISTS _grv_exact_gcs_reader");}catch(...) {}
        config.SetOptionByName("enable_external_access",Value(false));config.options.allowed_paths=std::move(paths);
        engine.connection.context->registered_state->Remove("grv.native.guard.v1");engine.connection.context->registered_state->Insert("grv.native.guard.v1",policy);
        if(capacity){std::strncpy(error,"trusted GCS reader initialization failed",capacity-1);error[capacity-1]=0;}return -1;
    }
}
extern "C" int grv_native_gcs_query(void *owner,const char *const *locations,size_t count,const char *query,unsigned char *output,size_t allowance,size_t *written,char *error,size_t capacity) {
    try {auto &engine=*static_cast<Owner *>(owner)->engine;
        if(count==0||count>4096||engine.snapshot_started||engine.result)throw PermissionException("invalid exact GCS scan lifecycle");
        vector<string> urls;for(size_t i=0;i<count;++i)urls.emplace_back(locations[i]);
        GCSScanScope active(engine,urls);
        if(grv_native_metadata_query(owner,query,output,allowance,written,error,capacity))throw IOException("guarded GCS query refused");
        if(std::chrono::steady_clock::now()>=engine.exact_gcs->deadline)throw PermissionException("GCS execution deadline expired");
        return 0;
    }catch(...){if(capacity){std::strncpy(error,"exact guarded GCS scan failed",capacity-1);error[capacity-1]=0;}return -1;}
}
