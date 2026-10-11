#include "duckdb.hpp"
using namespace duckdb;
#include "gcs_filesystem.inc"
#include <thread>
struct MockStock final:HTTPUtil {
    int requests=0;
    unique_ptr<HTTPResponse> SendRequest(BaseRequest &request,unique_ptr<HTTPClient> &) override {
        if(request.params.timeout==0 && request.params.timeout_usec==0)throw InternalException("unbounded timeout");
        ++requests;return make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
    }
};
int main(){
    try {
        MockStock stock;auto state=std::make_shared<ExactGCSState>();
        string url="https://storage.googleapis.com/grv-proof/data.parquet";
        state->configured=true;state->active=true;state->epoch=1;state->admitted.insert(url);
        state->deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(60);
        GRVHTTPRequestGuard::Register(stock,[state](BaseRequest &request,unique_ptr<HTTPClient> &client){state->BeforeRequest(request,client);});
        HTTPParams params(stock);HTTPHeaders headers;HeadRequestInfo first(url,headers,params);
        auto response=stock.Request(first);
        if(stock.requests!=1 || params.timeout!=0 || params.timeout_usec>60000)throw InternalException("request lease not bounded");
        HeadRequestInfo outside(url+"-outside",headers,params);
        try {stock.Request(outside);return 2;}catch(const PermissionException &){}
        PutRequestInfo write(url,headers,params,nullptr,0,"application/octet-stream");
        try {stock.Request(write);return 3;}catch(const PermissionException &){}
        // Simulate a multi-request stock open/read: the next internal request
        // must be fenced without requiring return to the filesystem wrapper.
        std::this_thread::sleep_for(std::chrono::milliseconds(90));
        HeadRequestInfo second(url,headers,params);
        try {stock.Request(second);return 4;}catch(const PermissionException &){}
        if(stock.requests!=1)return 5;
        GRVHTTPRequestGuard::Remove(stock);
        stock.Request(first); // independent ordinary stock/S3 utility unchanged
        return stock.requests==2?0:6;
    }catch(...){return 1;}
}
