#include <unistd.h>
#include <boost/lexical_cast.hpp>
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
    ping_proxy_map_.set_ping_dead_interval_sec(config_->ping_proxy_dead_time_);
    ping_proxy_map_.set_proxy_error_rate(config_->ping_proxy_error_rate_);
    ping_proxy_map_.set_proxy_error_cache_time(config_->ping_proxy_error_cache_time_);
    for(unsigned i = 0; i < config_->ping_nodes_lst_.size(); ++i)
        ping_proxy_map_.add_proxy(false, config_->ping_nodes_lst_[i].first, config_->ping_nodes_lst_[i].second);

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
    size_t idx = path.find(1, '/');
    if(idx != std::string::npos)
        path = path.substr(0, idx)
    // proxy ping
    if(path == config_->proxy_ping_path_)
    {
        update_ping_proxy(req->content.c_str());
        conn->write_http_ok();
        return;
    }
    // fetch request
    if(path == config_->fetch_task_path_)
    {
        recv_fetch_task(conn, BATCH_FETCH_REQUEST);
        return;
    }
    recv_fetch_task(conn, SINGLE_FETCH_REQUEST);
}

void SpiderService::handle_tunnel_request(conn_ptr_t conn)
{
    ServiceRequest* req = new ServiceRequest();
    req->conn_ = conn;
    req->state_= state;
    std::string err_msg;
    if(!acquire_proxy(req, err_msg))
    {
        delete req;
        conn->write_http_service_unavailable(err_msg);
        LOG_ERROR("acquire proxy error: %s for tunnel %s\n", err_msg.c_str(), conn->ToString().c_str());
        return;
    }
    delete req;
    http_server_.tunnel_connect(req->proxy_->ip_, req->proxy_->port_, true);
}

// proxy调度
bool SpiderService::acquire_proxy(ServiceRequest* service_req, std::string& err_msg_str)
{
    task_ptr_t ptask = service_req->task_;
    std::string parser_type;
    double parser_version = 0;
    bool fix_parser_version = false;
    std::string proxy_addr;
    if(ptask)
    {
        parser_type = ptask->parser_type_;
        parser_version = ptask->parser_version_;
        fix_parser_version = ptask->option_.fix_parser_version_;
        FetchRequest & fetch_req = ptask->req_array_[service_req->req_idx_];
        proxy_addr = fetch_req->proxy_addr_;
    }

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
    if(!parser_type.empty() || (service_req->task_ && service_req->task_->option_.ping_proxy_))
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
            service_req->proxy_ = proxy_map->acquire_foreign_proxy(parser_type, parser_version, fix_parser_version);
        // 墙内站点只使用国内的IP
        else if(service_req->task_ && !service_req->task_->option_.inwall_can_use_foreign_ip_)
            service_req->proxy_ = proxy_map->acquire_internal_proxy(parser_type, parser_version, fix_parser_version);
        else
            service_req->proxy_ = proxy_map->acquire_proxy(parser_type, parser_version, fix_parser_version);
    }

    if(service_req->proxy_ == NULL)
    {
        err_msg_str = "acquire_proxy error: ";
        if(service_req->task_ && service_req->task_->option_.ping_proxy_)
            err_msg_str += "ping ";
        err_msg_str += "proxy map ";
        if(!parser_type.empty())
        {
            err_msg_str += "parser:" + parser_type + ":" + 
                lexical_cast<std::string>(parser_version) + ":" + lexical_cast<std::string>(fix_parser_version);
        }
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
        ServiceRequest* service_req = new ServiceRequest();
        service_req->conn_ = conn;
        service_req->state_= state;
        std::string err_msg;
        if(!acquire_proxy(service_req, err_msg))
        {
            delete service_req;
            conn->write_http_service_unavailable(err_msg);
            LOG_ERROR("acquire proxy error: %s for %s\n", err_msg.c_str(), conn->ToString().c_str());
            return;
        }
        http_client_.PutRequest(conn->req_->Uri, (void*)service_req, &conn->req_->headers, 
            &conn->req_->content, single_task_cfg_, service_req->proxy_->acquire_addrinfo());
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
    if(!ptask->from_json(task_json) || ptask->req_array_.size() == 0)
    {
        std::string error_cont = "fetch task deserializate error";
        LOG_ERROR("skip invalid fetch task: %s, %s\n", 
            error_cont.c_str(), conn->peer_addr().c_str());
        conn->write_http_bad_request(error_cont);
        return;
    }
    std::string parser_type = ptask->parser_type_;
    double parser_version   = ptask->parser_version_;
    bool fix_parser_version = ptask->option_.fix_parser_version_;
    // 提交抓取任务
    for(unsigned i = 0; i < ptask->req_array_.size(); ++i)
    {
        FetchRequest& fetch_req = ptask->req_array_[i];
        ServiceRequest* service_req = new ServiceRequest();
        service_req->conn_ = conn;
        service_req->state_= state;
        service_req->task_ = ptask;
        service_req->req_idx_ = (int)i;
        std::string err_msg;
        if(!acquire_proxy(service_req, err_msg))
        {
            delete service_req;
            conn->write_http_service_unavailable(err_msg);
            LOG_ERROR("acquire proxy error: %s for %s\n", err_msg.c_str(), conn->ToString().c_str());
            continue;
        }
        http_client_.PutRequest(fetch_req.url_, (void*)service_req, &fetch_req.req_headers_, 
            &fetch_req->content_, batch_task_cfg_, service_req->proxy_->acquire_addrinfo());
    }
    // response ok
    conn->write_http_ok();
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

    ServiceRequest* service_req = (ServiceRequest*)result->contex_;
    task_ptr_t task = service_req->task_;
    conn_ptr_t conn = service_req->conn_;
    req_ptr_t  req  = conn->get_request();
    switch(service_req->state_)
    {
        case SINGLE_FETCH_REQUEST:
        {
            boost::shared_ptr<reply> resp(new reply());
            resp.status  = reply::ok;
            resp.headers = result->Headers;
            resp.content.assign(result->Body.begin(), result->Body.end());
            resp.status = result->resp_->StatusCode;
            conn->write_http_reply(resp);
            delete service_req;
            result->contex_ = NULL;
            break;
        }
        case BATCH_FETCH_REQUEST:
        {
            const FetchRequest& fetch_request = service_req->task_->req_array_[service_req->req_idx_];
            service_req->state_ = SEND_RESULT;
            ++service_req->send_result_retry_count_;
            fetch_request.req_headers_ = result->Headers;
            fetch_request.content_.swap(result->Body);
            http_client_.PutRequest(conn->req_->Uri, (void*)service_req, &fetch_request.req_headers_, 
                 &fetch_request.content_, batch_task_cfg_, service_req->task_->ai_);
            break;
        }
        case SEND_RESULT:
        {
            if(result->resp_->StatusCode != 200)
            {
                LOG_ERROR("send result to %s error.\n", task->get_response_address().c_str());
                ++service_req->send_result_retry_count_;
                if(service_req->send_result_retry_count_ <= config_->send_result_retry_times_)
                {
                    http_client_.PutRequest(conn->req_->Uri, (void*)service_req, &conn->req_->headers, 
                        conn->req_->content.c_str(), batch_task_cfg_, service_req->task_->ai_);
                    break;
                }
                LOG_ERROR("send result to %s failed.\n", task->get_response_address().c_str());
            }
            else
                LOG_DEBUG("send result to %s error.\n", task->get_response_address().c_str());
            delete service_req;
            break;
        }
        default:
        {
            LOG_ERROR("invalid service request state: %d\n", service_req->state_);
            assert(false);
        }
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
