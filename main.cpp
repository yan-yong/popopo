#include "SpiderService.hpp"
#include <boost/shared_ptr.hpp>

boost::shared_ptr<SpiderService> g_spider;

static void sig_quit_handler(int sig)
{
    g_spider->close();
}

int main(int argc, char* argv[])
{
    /* handle config */
    std::string config_file = "config.xml";
    if(argc == 1)
        LOG_INFO("Use default config file %s\n", config_file.c_str());
    else
        config_file = argv[1];
    boost::shared_ptr<Config> cfg(new Config(config_file.c_str()));
    cfg->ReadConfig();
    g_spider.reset(new SpiderService());
    g_spider->initialize(cfg);

    signal(SIGPIPE, SIG_IGN);
    struct sigaction quit_act, oact;
    sigemptyset(&quit_act.sa_mask);
    quit_act.sa_flags = 0;
    quit_act.sa_handler = sig_quit_handler;
    sigaddset(&quit_act.sa_mask, SIGINT);
    sigaddset(&quit_act.sa_mask, SIGTERM);
    if(sigaction(SIGINT, &quit_act, &oact) < 0)
    {
        LOG_ERROR("sigaction SIGINT error.\n");
        return -1;
    }
    if(sigaction(SIGTERM, &quit_act, &oact) < 0)
    {
        LOG_ERROR("sigaction SIGTERM error.\n");
        return -1;
    }
    g_spider->wait();

    return 0;
}
