#include <unistd.h>
#include "SpiderService.hpp"
#include "HttpClient.hpp"
#include "jsoncpp/include/json/json.h"
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

    ServiceRequest():
        state_(SINGLE_FETCH_REQUEST), proxy_(NULL)
    {

    }
};

SpiderService::SpiderService():
    single_task_cfg_(NULL), batch_task_cfg_(NULL), 
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

// proxy调度
bool SpiderService::acquire_proxy(ServiceRequest* service_req, std::string& err_msg_str, 
    const std::string& proxy_addr = std::string() )
{
    // 客户端指定proxy IP
    if(!proxy_addr.empty())
    {
        size_t sep_idx = proxy_addr.find(":");
        if(sep_idx != std::string::npos)
            proxy_addr = proxy_addr.substr(0, sep_idx);
        FetchProxy* proxy = outside_proxy_map_.acquire_proxy(proxy_addr);
        if(!proxy)
            proxy = ping_proxy_map_.acquire_proxy(proxy_addr);
        if(proxy)
        {
            service_req->proxy_ = proxy;
            return true;
        }
        err_msg_str = "cannot acquire fix proxy " + proxy_addr;
        return false;
    }

    size_t outside_proxy_size = outside_proxy_map_.size();
    size_t ping_proxy_size    = ping_proxy_map_.size();
    // 选择一个map
    FetchProxyMap* proxy_map  = NULL;
    if(service_req->task_ && service_req->task_->option_.ping_proxy_)
        proxy_map = &ping_proxy_map_;
    else
    {
        if(outside_proxy_size == 0)
            proxy_map = &ping_proxy_map_;
        else if(ping_proxy_size == 0)
            proxy_map = &outside_proxy_map_;
        // 两个size都不为0, 取一个最长时间没有访问过的map
        else
        {
            proxy_map = &ping_proxy_map_;
            if(ping_proxy_map_.min_refer_time() > outside_proxy_map_.min_refer_time())
                proxy_map = &outside_proxy_map_;
        }
    }

    if(proxy_map && proxy_map->size())
    {
        // 需要翻墙
        if(service_req->task_ && service_req->task_->option_.over_wall_)
            service_req->proxy_ = proxy_map->acquire_foreign_proxy();
        // 墙内站点只使用国内的IP
        else if(service_req->task_ && !service_req->task_->option_.inwall_can_use_foreign_ip_)
            service_req->proxy_ = proxy_map->acquire_internal_proxy();
        else
            service_req->proxy_ = proxy_map->acquire_proxy();
    }

    if(service_req->proxy_ == NULL)
    {
        err_msg_str = "acquire_proxy error: ";
        if(service_req->task_ && service_req->task_->option_.ping_proxy_)
            err_msg_str += "ping ";
        err_msg_str += "proxy map ";
        if(service_req->task_ && service_req->task_->option_.over_wall_)
            err_msg_str += " foreign list ";
        err_msg_str += "equal to 0";
        return false; 
    }

    return true;
}

void SpiderService::release_proxy(ServiceRequest* service_req)
{
    if(service_req->is_outside_)
        outside_proxy_map_.release_proxy(service_req->proxy_);
    else
        ping_proxy_map_.release_proxy(service_req->proxy_);
    service_req->proxy_ = NULL;
}

void SpiderService::recv_fetch_task(conn_ptr_t conn, ServiceState state)
{
    // 通过http代理接口 过来的请求
    if(state == SINGLE_FETCH_REQUEST)
    {
        ServiceRequest* req = new ServiceRequest();
        req->conn_ = conn;
        req->state_= state;
        std::string err_msg;
        if(!acquire_proxy(req, err_msg))
        {
            delete req;
            conn->write_http_service_unavailable(err_msg);
            LOG_ERROR("acquire proxy error: %s for %s\n", err_msg.c_str(), conn->ToString().c_str());
            return;
        }
        http_client_.PutRequest(conn->req_->Uri, (void*)req, &conn->req_->headers, 
            conn->req_->content.c_str(), single_task_cfg_, req->proxy_->AcquireAddrinfo());
        return;
    }
    
    //////// 通过http fetch_task接口 过来的请求  /////////
    req_ptr_t conn_req = conn->req_;
    if(conn_req->content_.empty())
    {
        std::string error_cont = "fetch task has no content";
        conn->write_http_bad_request(error_cont);
        LOG_ERROR("skip invalid fetch task: %s, %s\n", 
            error_cont.c_str(), conn->peer_addr().c_str());
        return;
    }
    // json解析
    const char*  task_str = conn_req->content_.c_str();
    Json::Value  task_json(Json::objectValue);
    Json::Reader reader;
    if(!reader.parse(task_str, task_json))
    {
        std::string error_cont = "fetch task content is invalid json";
        LOG_ERROR("skip invalid fetch task: %s, %s\n", 
            error_cont.c_str(), conn->peer_addr().c_str());
        conn->write_http_bad_request(error_cont);
        return;
    }
    // fetch task反序列化
    task_ptr_t ptask(new FetchTask());
    if(!ptask->from_json(task_json))
    {
        std::string error_cont = "fetch task deserializate error";
        LOG_ERROR("skip invalid fetch task: %s, %s\n", 
            error_cont.c_str(), conn->peer_addr().c_str());
        conn->write_http_bad_request(error_cont);
        return;
    }
    // 提交抓取任务
    for(unsigned i = 0; i < )
    {
        ServiceRequest* req = new ServiceRequest();
        http_client_.PutRequest(, , );
    }
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
