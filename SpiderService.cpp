#include <unistd.h>
#include <boost/lexical_cast.hpp>
#include "SpiderService.hpp"
#include "httpclient/HttpClient.hpp"
#include "jsoncpp/include/json/json.h"
#include "async_httpserver/reply.hpp"

uint64_t ServiceRequest::id_generator_ = 0;

SpiderService::SpiderService():
    outside_request_time_(0), single_task_cfg_(NULL), 
    batch_task_cfg_(NULL), result_push_cfg_(NULL), stopped_(false), 
    result_pid_(0), pool_pid_(0), outside_proxy_size_(0)
{

}

SpiderService::~SpiderService()
{
    close();
}

void SpiderService::initialize(boost::shared_ptr<Config> config)
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

    // dns resolver
    dns_resolver_.reset(new DNSResolver());

    // http client
    http_client_.reset(new HttpClient(config_->max_request_size_, 
        config_->max_result_size_, config_->client_bind_eth_.c_str()));
    http_client_->Open();

    // http server
    http_server_.reset(new HttpServer());
    http_server_->initialize("0.0.0.0", config_->listen_port_, config_->conn_timeout_sec_);
    http_server_->add_runtine(config_->outside_proxy_check_time_, boost::bind(&SpiderService::timed_runtine, shared_from_this()));
    http_server_->set_norm_http_handler(boost::bind(&SpiderService::handle_norm_request, this, _1));
    http_server_->set_tunnel_http_handler(boost::bind(&SpiderService::handle_tunnel_request, this, _1));

    // initialize work thread
    pthread_create(&result_pid_, NULL, result_thread, (void*)this);
    pthread_create(&pool_pid_, NULL, pool_thread, (void*)this);
    //usleep(10000);
}

void SpiderService::timed_runtine()
{
    time_t cur_time = time(NULL);
    if(outside_request_time_ + config_->outside_proxy_check_time_ < cur_time)
    {
        http_client_->PutRequest(config_->outside_proxy_obtain_uri_);
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

void SpiderService::wait()
{
    pthread_join(pool_pid_, NULL);
    pthread_join(result_pid_, NULL);
}

void SpiderService::handle_norm_request(conn_ptr_t conn)
{
    LOG_DEBUG("connection %s is coming\n", conn->peer_addr().c_str());
    request_ptr_t req = conn->get_request();
    std::string path = req->get_path();
    size_t idx = path.find(1, '/');
    if(idx != std::string::npos)
        path = path.substr(0, idx);
    //** proxy ping
    if(path == config_->proxy_ping_path_)
    {
        req->content.push_back('\0');
        update_ping_proxy(&req->content[0]);
        req->content.pop_back();
        conn->write_http_ok();
        return;
    }
    //** fetch request
    if(path == config_->fetch_task_path_)
        recv_fetch_task(conn, BATCH_FETCH_REQUEST);
    else
        recv_fetch_task(conn, SINGLE_FETCH_REQUEST);
}

void SpiderService::handle_tunnel_request(conn_ptr_t conn)
{
    ServiceRequest* req = new ServiceRequest();
    req->conn_ = conn;
    req->state_= SINGLE_FETCH_REQUEST;
    std::string err_msg;
    if(!do_fetch(req, err_msg))
    {
        LOG_ERROR("%s request %lu %s error: %s.\n", 
            req->get_state().c_str(), req->id_, req->get_url().c_str(), err_msg.c_str());
    }
    delete req;
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
        SpiderFetchRequest & fetch_req = ptask->req_array_[service_req->req_idx_];
        proxy_addr = fetch_req.proxy_addr_;
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

    size_t outside_proxy_size = outside_proxy_map_.candidate_size();
    size_t ping_proxy_size    = ping_proxy_map_.candidate_size();
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

    if(proxy_map && proxy_map->candidate_size())
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
                boost::lexical_cast<std::string>(parser_version) + ":" + boost::lexical_cast<std::string>(fix_parser_version);
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
    if(service_req->proxy_->is_outside_proxy())
        outside_proxy_map_.release_proxy(service_req->proxy_);
    else
        ping_proxy_map_.release_proxy(service_req->proxy_);
    service_req->proxy_ = NULL;
}

//** 接口: 执行抓取
bool SpiderService::do_fetch(ServiceRequest* request, std::string & err_msg)
{
    BatchConfig* batch_cfg = NULL; 
    switch(request->state_)
    {
        case SINGLE_FETCH_REQUEST:
        {
            batch_cfg = single_task_cfg_;
            request->fetch_request_retry_count_ += 1;
            break;
        }
        case TUNNEL_FETCH_REQUEST:
        {
            batch_cfg = tunnel_task_cfg_;
            request->fetch_request_retry_count_ += 1;
            break;
        }
        case BATCH_FETCH_REQUEST:
        {
            batch_cfg = batch_task_cfg_;
            request->fetch_request_retry_count_ += 1;
            break;
        }
        case SEND_RESULT:
        {
            batch_cfg = result_push_cfg_;
            request->send_result_retry_count_ += 1;
            break;
        }
    }
    if(batch_cfg && request->arrive_time_ + single_task_cfg_->timeout_sec_ < time(NULL))
    {
        err_msg = "fetch timeout";
        LOG_ERROR("%s request %lu %s error: %s.\n", 
            request->get_state().c_str(), request->id_, request->get_url().c_str(), err_msg.c_str());
        return false;
    }
    if(batch_cfg && (request->fetch_request_retry_count_ > batch_cfg->max_retry_times_ || 
        request->send_result_retry_count_ > batch_cfg->max_retry_times_) )
    {
        err_msg = "exceed max retry times";
        LOG_ERROR("%s request %lu %s error: %s.\n", 
            request->get_state().c_str(), request->id_, request->get_url().c_str(), err_msg.c_str());
        return false;
    }
    if(!acquire_proxy(request, err_msg))
    {
        LOG_ERROR("%s request %lu %s error: %s.\n", 
            request->get_state().c_str(), request->id_, request->get_url().c_str(), err_msg.c_str());
        return false;
    }

    LOG_INFO("put %s request %lu %s from %s\n", request->get_state().c_str(), request->id_,
        request->get_url().c_str(), request->conn_->peer_addr().c_str());
    // 隧道请求直接处理, 不使用httpclient
    if(request->state_ == TUNNEL_FETCH_REQUEST)
    {
        http_server_->tunnel_connect(request->conn_, request->proxy_->ip_, request->proxy_->port_, true);
        return true;
    }
    // 用httpclient请求
    http_client_->PutRequest(request->get_url(), (void*)request, request->get_headers(), 
        request->get_content(), batch_cfg, request->proxy_->acquire_addrinfo());
    return true;
}

//** 接口: 处理所有接收到的抓取任务
void SpiderService::recv_fetch_task(conn_ptr_t conn, ServiceState state)
{
    /////// 通过http代理接口 过来的请求
    if(state == SINGLE_FETCH_REQUEST)
    {
        ServiceRequest* service_req = new ServiceRequest();
        service_req->conn_  = conn;
        service_req->state_ = state;
        std::string err_msg;
        if(!do_fetch(service_req, err_msg))
        {
            conn->write_http_bad_request(err_msg);
            delete service_req;
        }
        return;
    }
    
    //////// 通过http fetch_task接口 过来的请求  /////////
    request_ptr_t conn_req = conn->get_request(); 
    if(conn_req->content.empty())
    {
        std::string err_msg = "fetch task has no content";
        conn->write_http_bad_request(err_msg);
        LOG_ERROR("skip invalid fetch task: %s, %s\n", 
            err_msg.c_str(), conn->peer_addr().c_str());
        return;
    }

    // json解析
    conn_req->content.push_back('\0');
    const char*  task_str = &conn_req->content[0];
    conn_req->content.pop_back();
    Json::Value  task_json(Json::objectValue);
    Json::Reader reader;
    if(!reader.parse(task_str, task_json))
    {
        std::string err_msg = "fetch task content is invalid json";
        LOG_ERROR("skip invalid fetch task: %s, %s\n", 
            err_msg.c_str(), conn->peer_addr().c_str());
        conn->write_http_bad_request(err_msg);
        return;
    }
    // fetch task反序列化
    task_ptr_t ptask(new SpiderFetchTask());
    if(!ptask->from_json(task_json) || ptask->req_array_.size() == 0)
    {
        std::string err_msg = "fetch task deserializate error";
        LOG_ERROR("BATCH_FETCH_REQUEST request %s error: %s.\n", 
            conn_req->uri.c_str(), err_msg.c_str());
        conn->write_http_bad_request(err_msg);
        return;
    }
    std::string parser_type = ptask->parser_type_;
    // 提交抓取任务
    for(unsigned i = 0; i < ptask->req_array_.size(); ++i)
    {
        ServiceRequest* service_req = new ServiceRequest();
        service_req->conn_ = conn;
        service_req->state_= state;
        service_req->task_ = ptask;
        service_req->req_idx_ = (int)i;
        std::string err_msg;
        if(!do_fetch(service_req, err_msg))
        {
            conn->write_http_bad_request(err_msg);
            delete service_req;
        }
    }
    // response ok
    conn->write_http_ok();
}

void SpiderService::update_ping_proxy(const char* proxy_str)
{
    Json::Value arr_value(Json::arrayValue);
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
        LOG_ERROR("outside proxy service result size %zd no change\n", outside_proxy_size_);
        return;
    }
    outside_proxy_size_ = value.size();
    outside_proxy_map_.update_outside_proxy(value);
    LOG_INFO("update outside proxy map success, candidate:%zd, error:%zd\n", 
        outside_proxy_map_.candidate_size(), outside_proxy_map_.error_size() );
}

//** 接口: 处理所有接收到的抓取结果
void SpiderService::handle_fetch_result(HttpClient::ResultPtr result)
{
    // 从proxy service接口拿到的代理列表结果
    if(!result->contex_)
    {
        if(result->error_.error_num() != RS_OK)
        {
            LOG_ERROR("request outside proxy service error: %s\n", GetSpiderError(result->error_).c_str());
            return;
        }
        result->resp_->Body.push_back('\0');
        result->resp_->Body.pop_back();
        update_outside_proxy(&result->resp_->Body[0]);
        return;
    }

    // 抓取错误，在未超时的情况下，应进行重试

    ServiceRequest* service_req = (ServiceRequest*)result->contex_;
    task_ptr_t task = service_req->task_;
    conn_ptr_t conn = service_req->conn_;
    request_ptr_t  req  = conn->get_request();
    switch(service_req->state_)
    {
        // proxy接口收到的抓取任务返回的结果
        case SINGLE_FETCH_REQUEST:
        {
            boost::shared_ptr<http::server4::reply> resp(new http::server4::reply());
            resp->status  = http::server4::reply::service_unavailable;
            if(result->resp_)
            {
                resp->headers = result->resp_->Headers;
                resp->content.assign(result->resp_->Body.begin(), result->resp_->Body.end());
                resp->status = (http::server4::reply::status_type)result->resp_->StatusCode;
            }
            conn->write_http_reply(resp);
            delete service_req;
            result->contex_ = NULL;
            break;
        }
        // 批量抓取任务返回的结果
        case BATCH_FETCH_REQUEST:
        {
            SpiderFetchRequest& fetch_request = service_req->task_->req_array_[service_req->req_idx_];
            service_req->state_ = SEND_RESULT;
            ++service_req->send_result_retry_count_;
            if(!result->resp_)
            {
                fetch_request.req_headers_ = result->resp_->Headers;
                fetch_request.content_.swap(result->resp_->Body);
            }
            http_client_->PutRequest(conn->get_request()->uri, (void*)service_req, &fetch_request.req_headers_, 
                 &fetch_request.content_, batch_task_cfg_, service_req->task_->ai_);
            break;
        }
        // 发送任务返回的结果
        case SEND_RESULT:
        {
            if(result->is_error())
            {
                LOG_ERROR("send result to %s error: %s, %hu / %u.\n", 
                    task->get_response_address().c_str(), result->error_msg().c_str(),
                    service_req->send_result_retry_count_, config_->send_result_retry_times_);
                ++service_req->send_result_retry_count_;
                if(service_req->send_result_retry_count_ <= config_->send_result_retry_times_)
                {
                    http_client_->PutRequest(conn->get_request()->uri, (void*)service_req, 
                        &conn->get_request()->headers, &conn->get_request()->content, 
                        batch_task_cfg_, service_req->task_->ai_);
                    break;
                }
                LOG_ERROR("send result to %s failed.\n", task->get_response_address().c_str());
            }
            else
                LOG_DEBUG("send result to %s ok.\n", task->get_response_address().c_str());
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

void* SpiderService::result_thread(void* arg)
{
    SpiderService* cur_obj = (SpiderService*) arg;
    HttpClient::ResultPtr result;
    while(!cur_obj->stopped_ && cur_obj->http_client_->GetResult(result))
        cur_obj->handle_fetch_result(result);
    return NULL;
}

void* SpiderService::pool_thread(void* arg)
{
    SpiderService* cur_obj = (SpiderService*) arg;
    cur_obj->http_server_->run();
    return NULL;
}
