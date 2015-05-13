#ifndef __SPIDER_SERVICE_HPP
#define __SPIDER_SERVICE_HPP
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/unordered_set.hpp>
#include "async_httpserver/httpserver.h"
#include "httpclient/HttpClient.hpp"
#include "Config.hpp"
#include "Proxy.hpp"
#include "lock/lock.hpp"

class SpiderService: public boost::enable_shared_from_this<SpiderService>
{
    shared_ptr<Config> config_;
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

    static void result_thread(void* arg);

    static void pool_thread(void* arg);

public:
    SpiderService(); 

    ~SpiderService();

    void initialize(shared_ptr<Config> config);

    void close();
};

#endif
