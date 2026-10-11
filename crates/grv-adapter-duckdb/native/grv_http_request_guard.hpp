#pragma once
#include "duckdb/common/http_util.hpp"
#include <functional>
#include <mutex>
#include <map>
namespace duckdb {
// Engine-side pre-request hook; signed HTTPFS clients/transport are unchanged.
// Registration is per stock util, not per credential or URL scope. A dedicated
// owner must revoke its registration before its utility is destroyed.
class GRVHTTPRequestGuard {
public:
    using Callback=std::function<void(BaseRequest &,unique_ptr<HTTPClient> &)>;
    static void Register(HTTPUtil &util,Callback callback){std::lock_guard<std::mutex> lock(Mutex());Registry()[&util]=std::move(callback);}
    static void Remove(HTTPUtil &util){std::lock_guard<std::mutex> lock(Mutex());Registry().erase(&util);}
    static void Before(HTTPUtil &util,BaseRequest &request,unique_ptr<HTTPClient> &client){
        Callback callback;
        {std::lock_guard<std::mutex> lock(Mutex());auto found=Registry().find(&util);if(found!=Registry().end())callback=found->second;}
        if(callback)callback(request,client);
    }
private:
    static std::mutex &Mutex(){static std::mutex mutex;return mutex;}
    static std::map<HTTPUtil *,Callback> &Registry(){static std::map<HTTPUtil *,Callback> registry;return registry;}
};
}
