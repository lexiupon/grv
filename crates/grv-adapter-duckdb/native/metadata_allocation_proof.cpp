// Narrow upstream allocator high-water evidence, NOT total heap/RSS accounting.
#include "shim.cpp"
#include <iostream>
struct AllocationCounters {
    std::mutex mutex;size_t live=0,peak=0,largest=0,budget=0,rejected=0;bool measuring=false;
    void Add(size_t size){
        std::lock_guard<std::mutex> guard(mutex);
        if(budget&&(live>budget||size>budget-live)){rejected=std::max(rejected,size);throw OutOfMemoryException("private proof allocator quota");}
        live+=size;if(measuring){peak=std::max(peak,live);largest=std::max(largest,size);}
    }
    void SetBudget(size_t bytes){std::lock_guard<std::mutex> guard(mutex);budget=bytes;rejected=0;}
    void Drop(size_t size){std::lock_guard<std::mutex> guard(mutex);live-=size;}
    void Begin(){std::lock_guard<std::mutex> guard(mutex);peak=live;largest=0;measuring=true;}
};
struct AllocationWitness : PrivateAllocatorData {
    std::shared_ptr<AllocationCounters> counters;
    explicit AllocationWitness(std::shared_ptr<AllocationCounters> counters):counters(std::move(counters)){}
};
data_ptr_t WitnessAllocate(PrivateAllocatorData *data,idx_t size){
    auto &witness=*data->Cast<AllocationWitness>().counters;witness.Add(size);
    try {return Allocator::DefaultAllocate(nullptr,size);}catch(...){witness.Drop(size);throw;}
}
void WitnessFree(PrivateAllocatorData *data,data_ptr_t ptr,idx_t size){Allocator::DefaultFree(nullptr,ptr,size);data->Cast<AllocationWitness>().counters->Drop(size);}
data_ptr_t WitnessReallocate(PrivateAllocatorData *data,data_ptr_t ptr,idx_t old,idx_t size){
    // Conservatively reserve old+new requested storage while realloc runs. The
    // proof quota covers forwarded request sizes, not actual allocator overhead.
    auto &witness=*data->Cast<AllocationWitness>().counters;witness.Add(size);
    data_ptr_t result;
    try {result=Allocator::DefaultReallocate(nullptr,ptr,old,size);}catch(...){witness.Drop(size);throw;}
    if(result)witness.Drop(old);else witness.Drop(size);return result;
}
void *MeasuredOwner(const char *path,std::shared_ptr<AllocationCounters> counters,const char *memory_limit="512MiB"){ 
    DBConfig config;
    config.allocator=make_uniq<Allocator>(WitnessAllocate,WitnessFree,WitnessReallocate,make_uniq<AllocationWitness>(std::move(counters)));
    config.SetOptionByName("memory_limit",Value(memory_limit));
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
    return new Owner{std::move(engine),std::move(interrupt)};
}
struct AllocationProofDirectory {
    string path;
    AllocationProofDirectory(){char pattern[]="/tmp/grv-metadata-allocation-XXXXXX";auto created=::mkdtemp(pattern);if(!created)throw IOException("private directory");path=created;}
    ~AllocationProofDirectory(){try{FileSystem::CreateLocal()->RemoveDirectory(path);}catch(...) {}}
};
int main(){
    AllocationProofDirectory directory;
    auto database_path=directory.path+"/proof.duckdb";
    // Standalone single-owner process. Do not substitute the global allocator
    // in shipping or in a process with any preexisting DuckDB consumers.
    auto original=Allocator::DefaultAllocatorReference();
    auto counters=std::make_shared<AllocationCounters>();auto &witness=*counters;
    auto data=make_uniq<AllocationWitness>(counters);
    auto tracked=make_shared_ptr<Allocator>(WitnessAllocate,WitnessFree,WitnessReallocate,std::move(data));
    Allocator::DefaultAllocatorReference()=tracked;
    char error[4096]={0};void *owner=MeasuredOwner(database_path.c_str(),counters);
    if(!owner){Allocator::DefaultAllocatorReference()=original;return 1;}
    int status=0;
    try {
        auto &engine=*static_cast<Owner *>(owner)->engine;engine.exact_gcs->configured=true;
        // An existing workspace can contain an oversized malformed durable
        // evidence row. Populate ONLY this disposable proof database directly;
        // do not imply shipping permits arbitrary generated SQL or this write.
        auto schema=engine.connection.Query("CREATE SCHEMA _grv");
        auto table=engine.connection.Query("CREATE TABLE _grv.pull_attempts(receipt VARCHAR)");
        auto stored=engine.connection.Query("INSERT INTO _grv.pull_attempts VALUES (repeat('x',40000000))");
        if(schema->HasError()||table->HasError()||stored->HasError())throw IOException("durable canary fixture");
        schema.reset();table.reset();stored.reset();
        vector<unsigned char> bytes(65536);size_t written=0;
        const char *queries[]={"SELECT i FROM range(1000000000) r(i)","SELECT repeat('x',6000000)","SELECT repeat('x',40000000)","SELECT receipt FROM _grv.pull_attempts LIMIT 2"};
        for(size_t index=0;index<4;++index){
            witness.Begin();auto baseline=witness.live;
            if(!grv_native_metadata_query(owner,queries[index],bytes.data(),bytes.size(),&written,error,sizeof(error)))throw IOException("oversize accepted");
            if(index>=2&&witness.largest<=32*1024*1024)throw IOException("expected upstream counterexample missing");
            std::cout<<"{\"trial\":"<<index<<",\"baseline\":"<<baseline<<",\"tracked_peak\":"<<witness.peak<<",\"largest_request\":"<<witness.largest<<",\"refused\":true}\n";
            if(engine.exact_gcs->failed)throw IOException("ordinary allocation refusal revoked");
        }
        // Candidate admission expressions: measure before adopting any of them
        // in production. bit_length/8 counts VARCHAR bytes, unlike length's
        // Unicode character count. Even a scalar result may allocate upstream.
        const char *admission[]={
            "SELECT bit_length(receipt)//8 FROM _grv.pull_attempts LIMIT 2",
            "SELECT CASE WHEN bit_length(receipt)//8>2093056 THEN 'oversized' ELSE receipt END FROM _grv.pull_attempts LIMIT 2",
            "SELECT receipt FROM _grv.pull_attempts WHERE bit_length(receipt)//8<=2093056 LIMIT 2",
            "SELECT CASE WHEN coalesce(bit_length(receipt)//8,0)>2093056 THEN error('durable metadata exceeds bounded record allowance') ELSE receipt END AS receipt FROM (SELECT receipt FROM _grv.pull_attempts LIMIT 2) AS _grv_bounded_metadata"
        };
        for(size_t index=0;index<4;++index){
            witness.Begin();auto baseline=witness.live;written=0;
            auto result=grv_native_metadata_query(owner,admission[index],bytes.data(),bytes.size(),&written,error,sizeof(error));
            if(bool(result)!=(index==3))throw IOException("admission expression outcome");
            std::cout<<"{\"admission_trial\":"<<index<<",\"baseline\":"<<baseline<<",\"tracked_peak\":"<<witness.peak<<",\"largest_request\":"<<witness.largest<<",\"result_bytes\":"<<written<<"}\n";
            if(index<3&&engine.exact_gcs->failed)throw IOException("admission expression revoked");
        }
        // Persist the source, release all engine caches, then repeat each
        // candidate with a freshly opened owner. This does not clear OS caches.
        grv_native_close(owner);owner=nullptr;
        for(size_t index=0;index<4;++index){
            owner=MeasuredOwner(database_path.c_str(),counters);if(!owner)throw IOException("reopen");
            auto &cold=*static_cast<Owner *>(owner)->engine;cold.exact_gcs->configured=true;
            witness.Begin();auto baseline=witness.live;written=0;
            auto result=grv_native_metadata_query(owner,admission[index],bytes.data(),bytes.size(),&written,error,sizeof(error));
            if(bool(result)!=(index==3))throw IOException("reopened admission outcome");
            if(witness.largest<=32*1024*1024)throw IOException("expected storage allocation counterexample missing");
            std::cout<<"{\"reopened_admission_trial\":"<<index<<",\"baseline\":"<<baseline<<",\"tracked_peak\":"<<witness.peak<<",\"largest_request\":"<<witness.largest<<",\"result_bytes\":"<<written<<"}\n";
            grv_native_close(owner);owner=nullptr;
        }
        // Test the existing engine setting separately from collector admission.
        // A pass/refusal here is evidence for this source path, not coverage of
        // every allocation or permission to reduce the shipping engine budget.
        owner=MeasuredOwner(database_path.c_str(),counters,"16MiB");
        static_cast<Owner *>(owner)->engine->exact_gcs->configured=true;
        witness.Begin();auto baseline=witness.live;written=0;
        auto limited=grv_native_metadata_query(owner,admission[0],bytes.data(),bytes.size(),&written,error,sizeof(error));
        std::cout<<"{\"engine_limit_trial\":0,\"memory_limit_bytes\":16777216,\"baseline\":"<<baseline<<",\"tracked_peak\":"<<witness.peak<<",\"largest_request\":"<<witness.largest<<",\"refused\":"<<(limited?"true":"false")<<"}\n";
        if(!limited||witness.largest>16777216)throw IOException("storage limit admission absent");
        grv_native_close(owner);owner=nullptr;
        owner=MeasuredOwner(database_path.c_str(),counters,"16MiB");
        auto &limited_engine=*static_cast<Owner *>(owner)->engine;limited_engine.exact_gcs->configured=true;
        if(grv_native_metadata_query(owner,"BEGIN",bytes.data(),bytes.size(),&written,error,sizeof(error))||grv_native_metadata_query(owner,"ROLLBACK",bytes.data(),bytes.size(),&written,error,sizeof(error)))throw IOException("source-free limit recovery");
        witness.Begin();baseline=witness.live;written=0;
        auto expression=grv_native_metadata_query(owner,queries[2],bytes.data(),bytes.size(),&written,error,sizeof(error));
        std::cout<<"{\"engine_limit_trial\":1,\"memory_limit_bytes\":16777216,\"baseline\":"<<baseline<<",\"tracked_peak\":"<<witness.peak<<",\"largest_request\":"<<witness.largest<<",\"refused\":"<<(expression?"true":"false")<<"}\n";
        if(!expression||witness.largest<=16777216)throw IOException("expression limit coverage counterexample missing");
        grv_native_close(owner);owner=nullptr;
        // Research-only request admission, before allocation, shared across
        // default and DBConfig paths. It deliberately does NOT cover STL,
        // signed-library independent allocators, runtime, or total RSS. No
        // production/helper replacement is introduced by this experiment.
        for(size_t index=0;index<2;++index){
            witness.SetBudget(0);owner=MeasuredOwner(database_path.c_str(),counters);
            static_cast<Owner *>(owner)->engine->exact_gcs->configured=true;
            witness.Begin();baseline=witness.live;written=0;witness.SetBudget(16*1024*1024);
            auto refused=grv_native_metadata_query(owner,index==0?admission[0]:queries[2],bytes.data(),bytes.size(),&written,error,sizeof(error));
            if(!refused||!witness.rejected||witness.peak>16*1024*1024)throw IOException("private allocator quota ineffective");
            std::cout<<"{\"request_quota_trial\":"<<index<<",\"quota_bytes\":16777216,\"baseline\":"<<baseline<<",\"tracked_peak\":"<<witness.peak<<",\"largest_admitted_request\":"<<witness.largest<<",\"largest_rejected_request\":"<<witness.rejected<<",\"refused\":true}\n";
            witness.SetBudget(0);grv_native_close(owner);owner=nullptr;
        }
        owner=MeasuredOwner(database_path.c_str(),counters);
        if(grv_native_metadata_query(owner,"SELECT 42",bytes.data(),bytes.size(),&written,error,sizeof(error)))throw IOException("quota source-free recovery");
    }catch(...){std::cerr<<"allocation witness failed (fixed private diagnostic)\n";status=1;}
    if(owner)grv_native_close(owner);Allocator::DefaultAllocatorReference()=original;return status;
}
