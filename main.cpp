#include "SpiderService.hpp"

int main()
{
    /* handle config */
    std::string config_file = "config.xml";
    if(argc == 1)
        LOG_INFO("Use default config file %s\n", config_file.c_str());
    else
        config_file = argv[1];
    g_cfg = new Config(config_file.c_str());
    g_cfg->ReadConfig();

}
