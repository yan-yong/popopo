#ifndef __FETCH_PROXY_HPP
#define __FETCH_PROXY_HPP
#include "log/log.h"
#include "proxy/Proxy.hpp"
#include "utility/stastic_count.h"
#include "jsoncpp/include/json/json.h"
#include "linklist/linked_list.hpp"

class FetchProxyMap;
class FetchProxy: Proxy
{
    //response cost time
    StasticCount<double, 10> resp_cost_ms_;
    StasticCount<double, 10> fail_rate_;
    struct addrinfo * addr_info_;    
    uint64_t digest_;

    time_t arrive_time_;
    time_t update_time_;
    time_t refer_time_; 
    uint32_t refer_cnt_;
    char     is_error_:   1;
    char     is_outside_: 1;

public:
    linked_list_node_t node_;

public:
    FetchProxy(bool is_outside_proxy = true, time_t cur_time = time(NULL)):
        addr_info_(NULL), digest_(0), arrive_time_(cur_time), update_time_(cur_time), 
        refer_time_(0), refer_cnt_(0), is_error_(0), 
        is_outside_(is_outside_proxy)
    {

    }

    ~FetchProxy()
    {
        if(addr_info_)
            delete addr_info_;
    }

    void add_resp_cost(double resp_cost_ms)
    {
        resp_cost_ms_.Add(resp_cost_ms);
    }

    void add_success()
    {
        fail_rate_.Add(0.0);
    }

    void add_fail()
    {
        fail_rate_.Add(1.0);
        ++err_num_; 
    }

    uint64_t get_digest() const
    {
        if(!digest_)
        {
            std::string proxy_str = ToString();
            MurmurHash_x64_64(proxy_str.c_str(), proxy_str.size(), &digest_);
        }
        return digest_; 
    }

    struct addrinfo* acquire_addrinfo() const
    {
        if(!addr_info_)
            addr_info_ = create_addrinfo(ip_, port_);
        return addr_info_;
    }

    friend class FetchProxyMap;
};

class FetchProxyMap
{
    typedef boost::unordered_map<uint64_t, FetchProxy*> proxy_map_t;
    typedef linked_list<FetchProxy, &FetchProxy::node_> proxy_list_t;
    typedef linked_list_map<time_t, FetchProxy, &FetchProxy::node_> error_map_t;
    static const double DEFAULT_ERROR_MIN_FAIL_RATE = 0.4; 
    static const time_t DEFAULT_ERROR_CACHE_TIME    = 36000; // 10 hours
    static const time_t DEFAULT_PING_INTERVAL_SEC   = 60;    // 1 minutes

    proxy_map_t  candidate_map_;
    proxy_list_t internal_lst_;
    proxy_list_t foreign_lst_;

    proxy_map_t  error_map_;
    error_map_t  error_lst_;
    Mutex        proxy_lock_;

    double error_min_fail_rate_;
    time_t error_cache_time_;
    time_t ping_dead_interval_sec_;

    FetchProxy* __acquire_proxy(proxy_list_t& pop_lst)
    {
        FetchProxy* ret  = pop_lst->get_front();
        if(ret)
        {
            ret->refer_time_ = time(NULL);
            ++ret->refer_cnt_;
            pop_lst->pop_front();
            pop_lst->add_back(*ret);
        }
        return ret;
    }

    void __move_to_error(FetchProxy* proxy, bool move_to_back = true)
    {
        uint64_t digest  = proxy->get_digest();
        if(!proxy->is_error_)
        {
            proxy->is_error_ = 1;
            candidate_map_.erase(digest);
        }
        error_map_[digest] = proxy;
        proxy_list_t::del(*proxy);
        if(!move_to_back)
            error_lst_.add_front(time(NULL), *proxy);
        else
            error_lst_.add_back(time(NULL), *proxy);
        std::string proxy_flag = proxy->is_outside_ ? "outside" : "ping";
        LOG_INFO("%s proxy %s move to error\n", proxy_flag.c_str(), proxy->ToString().c_str());
    }

    void __move_to_candidate(FetchProxy* proxy, bool move_to_back = true)
    {
        uint64_t digest  = proxy->get_digest();
        if(proxy->is_error_)
        {
            proxy->is_error_ = 0;
            error_map_.erase(digest);
        }
        //proxy->update_time_ = time(NULL);
        // 加入候选proxy中
        candidate_map_[digest] = proxy;
        proxy_list_t* plst = &internal_lst_;
        if(proxy->is_foreign_)
            plst = &foreign_lst_;
        error_map_t::del(*proxy);
        if(!move_to_back)
            plst->add_front(*proxy);
        else
            plst->add_back(*proxy);
        std::string proxy_flag = proxy->is_outside_ ? "outside" : "ping";
        LOG_INFO("%s proxy %s move to candidate\n", proxy_flag.c_str(), proxy->ToString().c_str());
    }

    void __check_error_proxy_delete()
    {
        time_t error_time;
        FetchProxy* proxy;
        time_t cur_time = time(NULL);
        while(true)
        {
            error_lst_.get_front(error_time, proxy);
            if(!proxy)
                return;
            if(error_time + error_cache_time_ > cur_time)
                break;
            if(proxy->refer_cnt_ != 0)
            {
                LOG_ERROR("try delete proxy %s fail: reference count %u != 0\n", 
                    proxy->ToString().c_str(), proxy->refer_cnt_);
                break;
            }
            uint64_t digest = proxy->get_digest();
            error_map_.erase(digest);
            delete proxy;
        }
    }

    void __check_dead_proxy_ping(proxy_list_t lst)
    {
        time_t cur_time = time(NULL);
        while(!lst.empty())
        {
            FetchProxy* proxy = lst.get_front();
            if(proxy->update_time_ + ping_dead_interval_sec_ > cur_time)
                break;
            if(proxy->refer_cnt_ == 0 && error_cache_time_ == 0)
            {
                lst.pop_front();
                delete proxy;
                LOG_INFO("delete dead ping proxy %s\n", proxy->ToString().c_str());
                continue;
            }
            __move_to_error(proxy);
        }
    }

public:
    FetchProxyMap():
        error_min_fail_rate_(DEFAULT_ERROR_MIN_FAIL_RATE),
        error_cache_time_(DEFAULT_ERROR_CACHE_TIME),
        ping_dead_interval_sec_(DEFAULT_PING_INTERVAL_SEC)
    {

    }

    ~FetchProxyMap()
    {
        for(proxy_map_t::iterator it = candidate_map_.begin(); 
            it != candidate_map_.end(); ++it)
        {
            delete it->second;
        }
        candidate_map_.clear();
        internal_lst_.clear();
        foreign_lst_.clear();
        for(proxy_map_t::iterator it = error_map_.begin(); 
                it != error_map_.end(); ++it)
        {
            delete it->second;
        }
        error_lst_.clear();
    }

    void set_proxy_error_rate(double error_rate)
    {
        error_min_fail_rate_ = error_rate;
    }

    void set_proxy_error_cache_time(time_t error_cache_time)
    {
        error_cache_time_ = error_cache_time;
    }

    void set_ping_dead_interval_sec(time_t ping_dead_interval_sec)
    {
        ping_dead_interval_sec_ = ping_dead_interval_sec;
    }

    size_t candidate_size() const
    {
        return candidate_map_.size();
    }

    size_t error_size() const
    {
        return error_map_.size();
    }

    time_t min_refer_time() const
    {
        time_t refer_time = 0;
        if(!internal_lst_.empty())
            refer_time = internal_lst_->get_front()->refer_time_;
        if(!foreign_lst_.empty() && refer_time > foreign_lst_->get_front()->refer_time_)
            refer_time = foreign_lst_->get_front()->refer_time_;
        return refer_time;
    }

    FetchProxy* acquire_proxy()
    {
        MutexGuard guard(proxy_lock_);
        proxy_list_t pop_lst = internal_lst_;
        if(!internal_lst_.empty() && !foreign_lst_.empty()
            && internal_lst_->get_front()->refer_time_ > foreign_lst_->get_front()->refer_time_)
        {
            pop_lst = foreign_lst_;
        }
        return __acquire_proxy(pop_lst);
    }

    FetchProxy* acquire_proxy(const std::string& ip)
    {
        uint64_t digest = 0;
        MurmurHash_x64_64(ip.c_str(), ip.size(), &digest);
        proxy_map_t::iterator it = candidate_map_.find(digest);
        if(it == candidate_map_.end())
            return NULL;
        FetchProxy* proxy = it->second;
        // 将proxy放到最后一个
        error_map_t::del(*proxy);
        proxy_list_t* plst = &internal_lst_;
        if(proxy->is_foreign_)
            plst = &foreign_lst_;
        plst->add_back(*proxy);
    } 

    void add_proxy(bool is_outside_proxy, const std::string& ip, uint16_t port)
    {
        MutexGuard guard(proxy_lock_);
        FetchProxy* proxy = new FetchProxy(is_outside_proxy);
        proxy->SetAddress(ip, port);
        __move_to_candidate(proxy, false);
    }

    FetchProxy* acquire_internal_proxy()
    {
        MutexGuard guard(proxy_lock_);
        return __acquire_proxy(internal_lst_);
    }

    FetchProxy* acquire_foreign_proxy()
    {
        MutexGuard guard(proxy_lock_);
        return __acquire_proxy(foreign_lst_);
    }

    void release_proxy(FetchProxy* proxy)
    {
        MutexGuard guard(proxy_lock_);
        --proxy->refer_cnt_;
        if(proxy->fail_rate_ >= DEFAULT_ERROR_MIN_FAIL_RATE && !proxy->is_error_)
            __move_to_error(proxy);
    }

    void update_outside_proxy(const Json::Value& proxy_json_array)
    {
        MutexGuard guard(proxy_lock_);
        time_t cur_time = time(NULL);
        Proxy* first_internal_proxy = internal_lst_.get_front();
        Proxy* first_foreign_proxy  = foreign_lst_.get_front();
        for(unsigned i = 0; i < proxy_json_array.size(); ++i)
        {
            Json::Value item  = proxy_json_array[i];
            FetchProxy* proxy = new FetchProxy(true, cur_time);
            proxy->FromJson(item);
            uint64_t digest = proxy->get_digest();
            proxy_map_t::iterator it = candidate_map_.find(digest);
            if(it != candidate_map_.end())
            {
                it->second->request_cnt_ = proxy->request_cnt_;
                it->second->http_enable_ = proxy->http_enable_;
                it->second->https_enable_= proxy->https_enable_;
                it->second->is_foreign_  = proxy->is_foreign_;
                it->second->update_time_ = cur_time;
                delete proxy;
                continue;
            }
            it = error_map_.find(digest);
            // 已在错误列表中了
            if(it != error_map_.end())
            {
                delete proxy;
                continue;
            }
            // 加入候选proxy中
            __move_to_candidate(proxy, false);
        }
        //检查没有更新的proxy, 并将其移入错误列表之中
        while(first_internal_proxy)
        {
            FetchProxy* next_proxy = internal_lst_.next(first_internal_proxy);
            if(first_internal_proxy->update_time_ != cur_time)
                __move_to_error(proxy);
            first_internal_proxy = next_proxy;
        }
        while(first_foreign_proxy)
        {
            FetchProxy* next_proxy = foreign_lst_.next(first_foreign_proxy);
            if(first_foreign_proxy->update_time_ != cur_time)
                __move_to_error(proxy);
            first_foreign_proxy = next_proxy;
        }
    }

    void remove_error_outside_proxy()
    {
        __check_error_proxy_delete();
    }

    void remove_dead_ping_proxy()
    {
        // 检查指定时间内没有ping的proxy, 并将其删除或移入错误列表中
        __check_dead_proxy_ping(internal_lst_);
        __check_dead_proxy_ping(foreign_lst_);
        __check_error_proxy_delete();
    } 

    void update_ping_proxy(const Json::Value& proxy_json_array)
    {
        MutexGuard guard(proxy_lock_);
        time_t cur_time = time(NULL);
        for(unsigned i = 0; i < proxy_json_array.size(); ++i)
        {
            Json::Value item  = proxy_json_array[i];
            FetchProxy* proxy = new FetchProxy(false, cur_time);
            proxy->FromJson(item);
            uint64_t digest = proxy->get_digest();
            proxy_map_t::iterator it = candidate_map_.find(digest);
            if(it != candidate_map_.end())
            {
                it->second->update_time_ = cur_time;
                proxy_map_t::del(*proxy);
                if(proxy->is_foreign_)
                    foreign_lst_.add_back(*proxy);
                else
                    internal_lst_.add_back(*proxy);
                delete proxy;
                continue;
            }
            it = err_map_.find(digest);
            if(it != err_map_.end())
            {
                // 更新时间
                it->second->update_time_ = cur_time;
                __move_to_candidate(it->second);
                delete proxy;
                continue;
            }
            __move_to_candidate(proxy);
        }
    }
};

#endif
