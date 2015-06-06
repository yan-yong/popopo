#ifndef __SPIDER_SERVICE_HPP
#define __SPIDER_SERVICE_HPP
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/unordered_set.hpp>
#include "async_httpserver/httpserver.h"
#include "httpclient/HttpClient.hpp"
#include "Config.hpp"
#include "proxy/Proxy.hpp"
#include "lock/lock.hpp"
#include "FetchProxy.hpp"
#include "async_httpserver/connection.hpp"
#include "FetchTask.hpp"

typedef boost::shared_ptr<FetchTask> task_ptr_t;

enum ServiceState
{
    SINGLE_FETCH_REQUEST,
    BATCH_FETCH_REQUEST,
    SEND_RESULT
};

struct ServiceRequest
{
    conn_ptr_t    conn_;
    task_ptr_t    task_;
    ServiceState  state_;
    FetchProxy*   proxy_;
    uint16_t      send_result_retry_count_;
    uint16_t      fetch_request_retry_count_;
    int           req_idx_;
    uint16_t      retry_count_;

    ServiceRequest():
        state_(SINGLE_FETCH_REQUEST), proxy_(NULL), send_result_retry_count_(0), 
        fetch_request_retry_count_(0), req_idx_(-1), retry_count_(0)
    {

    }
};

class SpiderService: public boost::enable_shared_from_this<SpiderService>
{
    boost::shared_ptr<Config> config_;
    boost::shared_ptr<HttpServer> http_server_;
    boost::shared_ptr<HttpClient> http_client_;

    FetchProxyMap outside_proxy_map_;
    FetchProxyMap ping_proxy_map_;
    time_t        outside_request_time_;

    // 单个的请求，是由http代理接口过来的请求
    BatchConfig*  single_task_cfg_;
    // 批量提交的请求 
    BatchConfig*  batch_task_cfg_;

    bool stopped_;
    pthread_t result_pid_;
    pthread_t pool_pid_;
    size_t    outside_proxy_size_;

protected:
    void timed_runtine();

    void handle_norm_request(conn_ptr_t);

    void handle_tunnel_request(conn_ptr_t);

    void handle_fetch_result(HttpClient::ResultPtr result);

    static void* result_thread(void* arg);

    static void* pool_thread(void* arg);

    void recv_fetch_task(conn_ptr_t conn, ServiceState state);

    void release_proxy(ServiceRequest* service_req);

    void update_ping_proxy(const char* proxy_str);

    void update_outside_proxy(const char* proxy_str);

    bool acquire_proxy(ServiceRequest* service_req, std::string& err_msg_str);

public:
    SpiderService(); 

    ~SpiderService();

    void initialize(boost::shared_ptr<Config> config);

    void close();

    void wait();
};

#endif
