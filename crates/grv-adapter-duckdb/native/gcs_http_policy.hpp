#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/common/exception.hpp"
#include "httpfs_client_pinned.hpp"
#include <functional>

namespace grv {

// Dedicated GCS parameter and request policy. Install AFTER verified stock
// HTTPFS LOAD and BEFORE opening any remote handles:
//   auto &stock = config.GetHTTPUtil(); // capture before SetHTTPUtil
//   config.SetHTTPUtil(duckdb::make_shared_ptr<GCSHTTPPolicy>(stock));
// DBConfig retains replaced utilities in old_http_utils, keeping stock alive.
// Do not install on a general-purpose engine, replace HTTPFS afterwards, or reuse
// handles opened before installation. This is not a URL/access-control fence.
class GCSHTTPPolicy final : public duckdb::HTTPUtil {
public:
    using Boundary=std::function<void(duckdb::BaseRequest &,duckdb::unique_ptr<duckdb::HTTPClient> &)>;
    explicit GCSHTTPPolicy(duckdb::HTTPUtil &stock, duckdb::string project = {},Boundary boundary={}) : stock_(stock), project_(std::move(project)),boundary_(std::move(boundary)) {
    }

    duckdb::string GetName() const override {
        return stock_.GetName();
    }

    // Cover every virtual entry point: none may fall back to base HTTPParams.
    duckdb::unique_ptr<duckdb::HTTPParams>
    InitializeParameters(duckdb::optional_ptr<duckdb::FileOpener> opener,
                         duckdb::optional_ptr<duckdb::FileOpenerInfo> info) override {
        return Apply(stock_.InitializeParameters(opener, info));
    }

    duckdb::unique_ptr<duckdb::HTTPParams>
    InitializeParameters(duckdb::DatabaseInstance &db, const duckdb::string &path) override {
        return Apply(stock_.InitializeParameters(db, path));
    }

    duckdb::unique_ptr<duckdb::HTTPParams>
    InitializeParameters(duckdb::ClientContext &context, const duckdb::string &path) override {
        return Apply(stock_.InitializeParameters(context, path));
    }

    // Preserve stock transport even for callers using the wrapper directly.
    duckdb::unique_ptr<duckdb::HTTPClient>
    InitializeClient(duckdb::HTTPParams &params, const duckdb::string &host) override {
        return stock_.InitializeClient(params, host);
    }
    void CloseClient(duckdb::unique_ptr<duckdb::HTTPClient> &&client) override {
        stock_.CloseClient(std::move(client));
    }
    duckdb::unique_ptr<duckdb::HTTPResponse>
    SendRequest(duckdb::BaseRequest &request, duckdb::unique_ptr<duckdb::HTTPClient> &client) override {
        if(!boundary_)return stock_.SendRequest(request, client);
        // Handles share params across parallel range reads. Never mutate that
        // shared object: each dispatch receives a private exact subtype copy.
        auto params=Copy(request.params.Cast<duckdb::HTTPFSParams>());
        struct DisposeClient {duckdb::unique_ptr<duckdb::HTTPClient> &value;~DisposeClient(){value.reset();}} dispose{client};
        duckdb::unique_ptr<duckdb::HTTPResponse> response;
        if(request.type==duckdb::RequestType::HEAD_REQUEST){
            duckdb::HeadRequestInfo local(request.url,request.headers,*params);
            local.try_request=request.try_request;boundary_(local,client);
            response=stock_.SendRequest(local,client);
        } else if(request.type==duckdb::RequestType::GET_REQUEST){
            auto &get=request.Cast<duckdb::GetRequestInfo>();
            duckdb::GetRequestInfo local(request.url,request.headers,*params,get.response_handler,get.content_handler);
            local.try_request=request.try_request;boundary_(local,client);
            response=stock_.SendRequest(local,client);
        } else {throw duckdb::PermissionException("GCS transport is read-only");}
        // Private clients can retain credentials; never park them in a handle
        // or extension pool for a subsequent request/epoch.
        return response;
    }
    void LogRequest(duckdb::BaseRequest &request, duckdb::optional_ptr<duckdb::HTTPResponse> response) override {
        stock_.LogRequest(request, response);
    }
    bool ShouldRetry(const duckdb::BaseRequest &request, const duckdb::HTTPResponse &response) override {
        return stock_.ShouldRetry(request, response);
    }

    static constexpr uint64_t MAX_TIMEOUT_SECONDS = 30;

private:
    duckdb::unique_ptr<duckdb::HTTPParams> Apply(duckdb::unique_ptr<duckdb::HTTPParams> params) {
        if (!params || &params->http_util != &stock_) {
            throw duckdb::InternalException("GCS HTTP policy requires stock-owned parameters");
        }
        // Official signed extensions privately link HTTPUtil::Request, so an
        // engine patch cannot intercept their nonvirtual call. Build an exact
        // pinned HTTPFSParams allocation owned by this virtual SendRequest
        // delegate; stock transport still receives its native subtype.
        if(boundary_)params=Copy(params->Cast<duckdb::HTTPFSParams>());
        params->follow_location = false;
        params->override_verify_ssl = true;
        params->verify_ssl = true;
        params->http_proxy.clear();
        params->http_proxy_port = 0;
        params->http_proxy_username.clear();
        params->http_proxy_password.clear();
        params->logger.reset();
        params->retries = 0;
        if (!project_.empty()) { params->extra_headers["x-goog-user-project"] = project_; }
        // Zero means unbounded in stock clients. Keep shorter finite timeouts.
        if ((params->timeout == 0 && params->timeout_usec == 0) ||
            params->timeout >= MAX_TIMEOUT_SECONDS) {
            params->timeout = MAX_TIMEOUT_SECONDS;
            params->timeout_usec = 0;
        }
        return params;
    }

    struct PrivateParams final : duckdb::HTTPFSParams {
        explicit PrivateParams(duckdb::HTTPUtil &util):HTTPFSParams(util){}
        ~PrivateParams() override {volatile char *data=bearer_token.empty()?nullptr:&bearer_token[0];for(size_t i=0;i<bearer_token.size();++i)data[i]=0;}
    };
    duckdb::unique_ptr<duckdb::HTTPFSParams> Copy(const duckdb::HTTPFSParams &p) {
        auto fresh=duckdb::make_uniq<PrivateParams>(*this);
        fresh->timeout=p.timeout;fresh->timeout_usec=p.timeout_usec;fresh->retries=p.retries;
        fresh->retry_wait_ms=p.retry_wait_ms;fresh->retry_backoff=p.retry_backoff;fresh->keep_alive=p.keep_alive;
        fresh->follow_location=p.follow_location;fresh->override_verify_ssl=p.override_verify_ssl;fresh->verify_ssl=p.verify_ssl;
        fresh->http_proxy=p.http_proxy;fresh->http_proxy_port=p.http_proxy_port;fresh->http_proxy_username=p.http_proxy_username;fresh->http_proxy_password=p.http_proxy_password;
        fresh->extra_headers=p.extra_headers;fresh->logger=p.logger;
        fresh->force_download=p.force_download;fresh->auto_fallback_to_full_download=p.auto_fallback_to_full_download;
        fresh->enable_server_cert_verification=p.enable_server_cert_verification;fresh->enable_curl_server_cert_verification=p.enable_curl_server_cert_verification;
        fresh->hf_max_per_page=p.hf_max_per_page;fresh->ca_cert_file=p.ca_cert_file;fresh->bearer_token=p.bearer_token;
        fresh->unsafe_disable_etag_checks=p.unsafe_disable_etag_checks;fresh->s3_version_id_pinning=p.s3_version_id_pinning;fresh->state=p.state;
        fresh->user_agent=p.user_agent;fresh->pre_merged_headers=p.pre_merged_headers;fresh->force_download_threshold=p.force_download_threshold;
        return fresh;
    }
    duckdb::HTTPUtil &stock_;
    duckdb::string project_;
    Boundary boundary_;
};

} // namespace grv
