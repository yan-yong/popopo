#include <unistd.h>
#include "SpiderService.hpp"
#include "HttpClient.hpp"
#include "jsoncpp/include/json/json.h"

enum ServiceState
{
    FETCH_REQUEST,
    SEND_RESULT
};

struct ServiceRequest
{
    request_ptr_t req_;
    conn_ptr_t    conn_;
    FetchProxy*   proxy_;
    std::string   result_dest_;
    ServiceState  state_;
};

SpiderService::SpiderService():
    outside_batch_cfg_(NULL), inside_batch_cfg_(NULL), 
    stopped_(false), result_pid_(0), pool_pid_(0), 
    outside_proxy_size_(0), outside_request_time_(0)
{

}

void SpiderService::initialize(shared_ptr<Config> config)
{
    config_ = config;

    // outside proxy
    outside_proxy_map_.set_proxy_error_rate(config_->outside_proxy_error_rate_);
    outside_proxy_map_.set_proxy_error_cache_time(config_->outside_proxy_error_cache_time_);

    // inside proxy
    ping_proxy_map_.set_ping_dead_interval_sec(config_->inside_proxy_dead_time_);
    ping_proxy_map_.set_proxy_error_rate(config_->inside_proxy_error_rate_);
    ping_proxy_map_.set_proxy_error_cache_time(config_->inside_proxy_error_cache_time_);

    // http client
    http_client_.reset(new HttpClient(config_->max_request_size_, 
        config_->max_result_size_, config_->bind_eth_));
    http_client_->Open();

    // http server
    http_server_.reset(new HttpServer());
    http_server_->initialize("0.0.0.0", config_->listen_port_, config_->conn_timeout_sec_);
    http_server_->add_runtine(5, boost::bind(&SpiderService::timed_runtine, shared_from_this()));

    // initialize work thread
    boost::shared_ptr<SpiderService> p_cur = new boost::shared_ptr<SpiderService>(shared_from_this());
    pthread_create(&pool_pid_, NULL, result_thread, (void*)p_cur);
    pthread_create(&result_pid_, NULL, pool_thread, (void*)p_cur);
    usleep(10000);
    delete p_cur; 
}

void SpiderService::timed_runtine()
{
    time_t cur_time = time(NULL);
    if(outside_request_time_ + config_->outside_proxy_check_time_ < cur_time)
    {
        http_client_->PutRequest(config_->proxy_service_uri_);
        outside_request_time_ = cur_time;
    }
    ping_proxy_map_.remove_dead_ping_proxy();
    outside_proxy_map_.remove_error_outside_proxy();
}

void SpiderService::close()
{
    if(stopped_)
        return;
    stopped_ = true; 
    http_client_->Close();
    http_server_->stop(); 
}

void SpiderService::handle_norm_request(conn_ptr_t conn)
{
    request_ptr_t req = conn->get_request();
    std::string path = req->get_path();
    // proxy ping
    if(path.find(config_->proxy_ping_path_) == 0)
    {
        update_ping_proxy(req->content.c_str());
        conn->write_http_ok();
        return;
    }
    // fetch request
    if(path.find(config_->fetch_task_path_) == 0)
    {
        return;
    }
}

void SpiderService::handle_tunnel_request(conn_ptr_t conn)
{
    http_server_->remove_connection(conn);
}

void SpiderService::update_ping_proxy(const char* proxy_str)
{
    Json::Value arr_value(Json::arrayValue)
    Json::Reader reader;
    Json::Value value;
    if(!reader.parse(proxy_str, value))
    {
        LOG_ERROR("ping proxy service result parse error\n");
        return;
    }

    arr_value.append(value);
    ping_proxy_map_.update_ping_proxy(arr_value);
    LOG_INFO("update ping proxy map success, candidate:%zd, error:%zd\n", 
        ping_proxy_map_.candidate_size(), ping_proxy_map_.error_size() );
}

void SpiderService::update_outside_proxy(const char* proxy_str)
{
    Json::Reader reader;
    Json::Value value;
    if(!reader.parse(proxy_str, value))
    {
        LOG_ERROR("outside proxy service result parse error\n");
        return;
    }
    if(value.size() == 0)
    {
        LOG_ERROR("outside proxy service result 0 item\n");
        return;
    }
    if(value.size() == outside_proxy_size_)
    {
        LOG_ERROR("outside proxy service result size %d no change\n", outside_proxy_size_);
        return;
    }
    outside_proxy_size_ = value.size();
    outside_proxy_map_.update_outside_proxy(value);
    LOG_INFO("update outside proxy map success, candidate:%zd, error:%zd\n", 
        outside_proxy_map_.candidate_size(), outside_proxy_map_.error_size() );
}

void SpiderService::handle_fetch_result(HttpClient::ResultPtr result)
{
    // 从proxy service接口拿到的代理信息
    if(!result->contex_)
    {
        if(result_->error_ != RS_OK)
        {
            LOG_ERROR("request outside proxy service error: %s\n", GetSpiderError(result_->error_));
            return;
        }
        result_->resp_->Body.push_back('\0');
        result_->resp_->Body.pop_back();
        update_outside_proxy(&result_->resp_->Body[0]);
    }
}

void SpiderService::result_thread(void* arg)
{
    typedef boost::shared_ptr<SpiderService> obj_ptr_t;
    obj_ptr_t cur = *(obj_ptr_t*)arg;
    HttpClient::ResultPtr result;
    while(!stopped_ && cur->http_client_->GetResult(result))
        handle_fetch_result(result);
}

void SpiderService::pool_thread(void* arg)
{
    typedef boost::shared_ptr<SpiderService> obj_ptr_t;
    obj_ptr_t cur = *(obj_ptr_t*)arg;
    cur->http_server_->run();
}
