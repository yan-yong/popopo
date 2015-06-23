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
#include "SpiderFetchTask.hpp"

typedef boost::shared_ptr<SpiderFetchTask> task_ptr_t;

enum ServiceState
{
    SINGLE_FETCH_REQUEST,     // 代理接口提交的http请求
    TUNNEL_FETCH_REQUEST,     // 代理接口提交的https请求
    BATCH_FETCH_REQUEST,      // 批量接口提交的多个请求
    SEND_RESULT               // 结果推送请求
};

// 收到的抓取请求
struct ServiceRequest
{
    static uint64_t id_generator_;
    uint64_t      id_;
    conn_ptr_t    conn_;
    task_ptr_t    task_;
    ServiceState  state_;
    FetchProxy*   proxy_;
    uint16_t      send_result_retry_count_;
    uint16_t      fetch_request_retry_count_;
    int           req_idx_;
    time_t        arrive_time_;

    ServiceRequest():
        state_(SINGLE_FETCH_REQUEST), proxy_(NULL), 
        send_result_retry_count_(0), fetch_request_retry_count_(0), 
        req_idx_(-1), arrive_time_(time(NULL))
    {
        id_ = __sync_fetch_and_add(&id_generator_, 1);
    }

    request_ptr_t get_request() const
    {
        return conn_->get_request();
    } 
 
    std::string get_url() const
    {
        return conn_->get_request()->uri; 
    }

    std::string get_method() const
    {
        return conn_->get_request()->method;
    }

    const MessageHeaders* get_headers() const
    {
        return &conn_->get_request()->headers;
    }

    const std::vector<char>* get_content() const
    {
        if(conn_->get_request()->method == "GET")
            return NULL;
        return &conn_->get_request()->content;
    }

    std::string get_state() const
    {
        switch(state_)
        {
            case SINGLE_FETCH_REQUEST:
                return "SINGLE_FETCH_REQUEST";
            case BATCH_FETCH_REQUEST:
                return "BATCH_FETCH_REQUEST";
            case TUNNEL_FETCH_REQUEST:
                return "TUNNEL_FETCH_REQUEST";
            case SEND_RESULT:
                return "SEND_RESULT";
        }
        return "";
    }
};

class SpiderService: public boost::enable_shared_from_this<SpiderService>
{
    boost::shared_ptr<Config>      config_;
    boost::shared_ptr<HttpServer>  http_server_;
    boost::shared_ptr<HttpClient>  http_client_;

    FetchProxyMap outside_proxy_map_;
    FetchProxyMap ping_proxy_map_;
    time_t        outside_request_time_;

    // 批次配置：单个的请求，由http代理接口过来的请求
    BatchConfig*  single_task_cfg_;
    // 批次配置：隧道请求，有https代理接口过来的请求
    BatchConfig*  tunnel_task_cfg_;
    // 批次配置：批量提交的请求
    BatchConfig*  batch_task_cfg_;
    // 发送结果：结果推送
    BatchConfig*  result_push_cfg_;

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

    bool do_fetch(ServiceRequest* request, std::string & err_msg);

public:
    SpiderService(); 

    ~SpiderService();

    void initialize(boost::shared_ptr<Config> config);

    void close();

    void wait();
};

#endif
