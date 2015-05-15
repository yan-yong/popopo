#ifndef __CONFIG_HPP
#define __CONFIG_HPP
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp> 
#include <string>
#include <boost/regex.hpp>
#include "utility/string_utility.h"
#include "utility/net_utility.h"

struct Config
{
    std::string config_file_;

    std::string listen_port_;
    time_t      conn_timeout_sec_;
    std::string proxy_ping_path_;
    std::string fetch_task_path_; 

    std::string outside_proxy_obtain_uri_;
    time_t outside_proxy_check_time_;
    double outside_proxy_error_rate_;
    time_t outside_proxy_error_cache_time_;

    time_t ping_proxy_dead_time_;
    double ping_proxy_error_rate_;
    time_t ping_proxy_error_cache_time_; 
    std::vector< std::pair<std::string, uint16_t> > ping_nodes_lst_;

    std::string client_bind_eth_;
    std::string client_bind_ip_;
    size_t      max_request_size_;
    size_t      max_result_size_;


public:
    Config(const char* config_file)
    {
        config_file_ = config_file; 
    }

    int ReadConfig()
    {
        boost::property_tree::ptree pt; 
        read_xml(config_file_, pt);

        listen_port_ = pt.get<std::string>("Root.HttpServer.ListenPort");
        conn_timeout_sec_ = pt.get<time_t>("Root.HttpServer.ConnectTimeoutSec");
        proxy_ping_path_ = pt.get<std::string>("Root.RequestPath.ProxyPing"); 
        fetch_task_path_ = pt.get<std::string>("Root.RequestPath.FetchTask");

        outside_proxy_obtain_uri_ = pt.get<std::string>("Root.OutsideProxy.Uri");
        outside_proxy_check_time_ = pt.get<time_t>("Root.OutsideProxy.CheckIntervalSec");
        outside_proxy_error_rate_ = pt.get<double>("Root.OutsideProxy.ProxyErrorRate");
        outside_proxy_error_cache_time_ = pt.get<time_t>("Root.OutsideProxy.ErrorCacheTimeout");

        ping_proxy_dead_time_  = pt.get<time_t>("Root.PingProxy.DeadIntervalSec");
        ping_proxy_error_rate_ = pt.get<double>("Root.PingProxy.ProxyErrorRate");
        ping_proxy_error_cache_time_ = pt.get<time_t>("Root.PingProxy.ErrorCacheTimeout");
        // 固定指定nodes的列表
        std::string nodes_str = pt.get<std::string>("Root.PingProxy.Nodes");
        boost::regex expression("(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)");
        boost::smatch what;
        std::string::const_iterator start = nodes_str.begin();
        std::string::const_iterator end   = nodes_str.end();
        while( boost::regex_search(start, end, what, expression) )
        {  
            std::pair<std::string, uint16_t> ip_port;
            if(what.size() != 2 || !what[0].matched || !what[1].matched)
                continue;
            std::string ip   = what[0];
            std::string port = what[1];
            ping_nodes_lst_.push_back(std::make_pair(ip, (uint16_t)atoi(port.c_str())));
            start = what[1].second;
        }

        client_bind_eth_ = pt.get<std::string>("Root.HttpClient.EthName");
        get_local_address(client_bind_eth_, client_bind_ip_);
        max_request_size_= pt.get<size_t>("Root.HttpClient.MaxRequestSize");
        max_result_size_ = pt.get<size_t>("Root.HttpClient.MaxResultSize");

        return 0;
    }
};

#endif
