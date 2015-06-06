#ifndef __FETCH_REQUEST_HPP
#define __FETCH_REQUEST_HPP
#include "log/log.h"
#include "jsoncpp/include/json/json.h"
#include "httpparser/HttpMessage.hpp"
#include "httpclient/SchedulerTypes.hpp"

struct FetchRequest
{
    std::string url_;
    std::vector<char> content_;
    // 指定下载结点, 格式为ip:port
    std::string proxy_addr_;
    MessageHeaders req_headers_; 
 
public:
    void add_http_header(std::string key, std::string val)
    {
        req_headers_.Add(key, val);
    }

    void set_download_proxy(std::string proxy_addr)
    {
        proxy_addr_ = proxy_addr;
    }

    Json::Value to_json() const
    {
        Json::Value val;
        val["url"]  = url_;
        val["cont"] = std::string(content_.begin(), content_.end());
        Json::Value heads(Json::arrayValue);
        for(unsigned i = 0; i < req_headers_.Size(); ++i)
        {
            Json::Value item(Json::objectValue);
            item[req_headers_[i].Name] = req_headers_[i].Value;
            heads.append(item);
        }
        if(req_headers_.Size() > 0)
            val["head"] = heads;
        if(!proxy_addr_.empty())
            val["proxy"] = proxy_addr_;
        return val;
    }

    bool from_json(const Json::Value & json_val)
    {
        Json::Value empty_val;
        Json::Value url_obj = json_val.get("url", empty_val);
        if(url_obj.empty())
        {
            LOG_ERROR("\n");
            return false;
        }
        url_ = url_obj.asString();
        Json::Value content_obj = json_val.get("cont", empty_val);
        if(!content_obj.empty())
        {
            std::string content_str = content_obj.asString();
            content_.assign(content_str.begin(), content_str.end());
        }
        Json::Value proxy_obj = json_val.get("proxy", empty_val);
        if(!proxy_obj.empty())
            proxy_addr_ = proxy_obj.asString();
        Json::Value head_obj = json_val.get("head", empty_val);
        if(head_obj.empty())
            return true;
        Json::Value::Members members = head_obj.getMemberNames();
        for (Json::Value::Members::iterator it = members.begin(); it != members.end(); ++it)
            req_headers_.Add(*it, head_obj[*it].asString());
        return true;
    }
};

/******** option 选项 **********/
struct TaskOption
{
    // 抓取优先级 (1-9)
    ResourcePriority prior_;
    // 0: 抓取任务无需翻墙 
    // 1: 要求国外的抓取结点, 即抓取的url需要翻墙
    char over_wall_:           1;
    // 1: 要求只使用我们自己部署的结点
    char ping_proxy_:          1;
    // 0: 表示解析器版本为指定的最低版本
    // 1: 表示解析器版本需要精确为parser_version_
    char fix_parser_version_:  1;
    // 0: 墙内站点只能使用国内Proxy
    // 1: 墙内站点可以使用国外Proxy
    char inwall_can_use_foreign_ip_:  1;
};

struct FetchTask
{
    static const double REQUEST_VERSION = 1.0;
    // 请求的版本号
    double ver_;
    // 结果response地址
    struct addrinfo* ai_;
    // 抓取请求列表
    std::vector<FetchRequest> req_array_;
    // 解析器类型
    std::string parser_type_;
    // 解析器的版本号, 0表示任何版本都ok
    double parser_version_;

    union
    {
        TaskOption option_;
        uint64_t   op_val_;   
    };

    FetchTask(): ver_(REQUEST_VERSION), ai_(NULL), parser_version_(0), op_val_(0) 
    {

    }

    ~FetchTask()
    {
        if(ai_)
        {
            freeaddrinfo(ai_);
            ai_ = NULL;
        }
    }

    //FIXME: 可以设置多个ip和端口
    void set_response_address(const char* ip, uint16_t port)
    {
        if(ai_)
            freeaddrinfo(ai_);
        ai_ = create_addrinfo(ip, port);
    }

    std::string get_response_address()
    {
        // 当response地址有多个时，使用轮询的方式进行
        struct addrinfo* head_ai = ai_;
        struct addrinfo* tail_ai = ai_;
        while(tail_ai->ai_next)
            tail_ai = tail_ai->ai_next;
        if(tail_ai != ai_)
        {
            ai_ = ai_->ai_next;
            tail_ai->ai_next = head_ai;
            head_ai->ai_next = NULL;
        }
        char ip_str[100];
        uint16_t port = 0;
        get_addr_string(head_ai->ai_addr, ip_str, 100, port);
        char buf[100];
        snprintf(buf, 100, "%s:%hu", ip_str, port);
        return buf;
    }

    void add_request(const FetchRequest& req)
    {
        req_array_.push_back(req);
    }

    Json::Value to_json() const
    {
        Json::Value val(Json::objectValue);
        // version
        char version_str[10];
        snprintf(version_str, 10, "%f", ver_);
        val["ver"]  = version_str;
        // response address
        char ip_str[20];
        uint16_t port = 0;
        if(!get_addr_string(ai_->ai_addr, ip_str, 20, port))
            return val;
        char addr_str[100];
        snprintf(addr_str, 100, "%s:%hu", ip_str, port);
        val["addr"] = addr_str;

        // option
        char option_buf[10];
        snprintf(option_buf, 10, "0x%lx", op_val_);
        val["op"] = option_buf;

        // parser info
        if(!parser_type_.empty())
        {
            val["pt"] = parser_type_;
            if(parser_version_ != 0)
            {
                char version_str[10];
                snprintf(version_str, 10, "%f", parser_version_);
                val["pv"] = version_str;
            }
        }
        // fetch request
        Json::Value req_val(Json::arrayValue);
        for(unsigned i = 0; i < req_array_.size(); ++i)
            req_val.append(req_array_[i].to_json());
        val["req"]  = req_val;
        return val;
    }
    
    bool from_json(const Json::Value& val)
    {
        Json::Value empty_val;
        // version
        Json::Value ver_obj = val.get("ver", empty_val);
        if(!ver_obj)
            ver_ = atof(ver_obj.asString().c_str());
        // response address
        Json::Value addr_obj = val.get("addr", empty_val);
        if(addr_obj.empty())
            return false;
        std::string addr_str = addr_obj.asString();
        size_t sep_idx       = addr_str.find(":");
        if(sep_idx == std::string::npos || sep_idx == addr_str.size() - 1)
            return false;
        std::string ip_str   = addr_str.substr(0, sep_idx);
        std::string port_str = addr_str.substr(sep_idx + 1);
        uint16_t port = atoi(port_str.c_str());
        if(ai_)
            freeaddrinfo(ai_);
        ai_ = create_addrinfo(ip_str.c_str(), port);

        // task option
        Json::Value op_obj = val.get("op", empty_val);
        if(!op_obj.empty())
            op_val_ = strtoul(op_obj.asCString(), NULL, 16);

        // parser info
        Json::Value parser_type_obj = val.get("pt", empty_val);
        if(!parser_type_obj.empty())
        {
            parser_type_    = parser_type_obj.asString();
            option_.ping_proxy_ = 1;
        }
        Json::Value parser_version_obj = val.get("pv", empty_val);
        if(!parser_version_obj.empty())
        {
            parser_version_ = atof(parser_version_obj.asCString());
            if(parser_type_.empty())
                return false;
        }

        // fetch request
        Json::Value req_obj  = val.get("req", empty_val);
        if(req_obj.empty())
            return false;
        for(unsigned i = 0; i < req_obj.size(); ++i)
        {
            FetchRequest req;
            if(!req.from_json(req_obj[i]))
                continue;
            req_array_.push_back(req);
        }
        return true; 
    }
};

#endif
