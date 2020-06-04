#include"redisclient.h"

#include"base_library/log.h"

#include<string>
#include<cstdint>

#include<unistd.h>

using namespace GBDownLinker;

int main()
{
	std::string logfile="test_redisclient.log";
    base::CLog::Instance().Init(logfile);
	
	const std::string redis_ip="192.168.12.59";

	RedisClient client;
	
	REDIS_SERVER_LIST redis_server_list;
	RedisServerInfo server1(redis_ip, 26379);
	redis_server_list.push_back(server1);	
	RedisServerInfo server2(redis_ip, 26380);
	redis_server_list.push_back(server2);	
	RedisServerInfo server3(redis_ip, 26381);
	redis_server_list.push_back(server3);

	std::string master_name = "mymaster";
	uint32_t connect_timeout_ms = 2000;
	uint32_t read_timeout_ms = 3500;
	std::string passwd = "";
	uint32_t master_connection_num = 2;

	RedisClientInitResult init_result = client.init(SENTINEL_MODE, redis_server_list, master_name, master_connection_num, connect_timeout_ms, read_timeout_ms, passwd);
	if(init_result!=InitSuccess)
	{
		return -1;
	}

	
	// test case 1
	client.TestHiredisGetReply();

	// test case 2
	while(true)
	{
		LOG_WRITE_INFO("loop, to test subscribe +switch-master");
		sleep(20);
	}

	LOG_WRITE_INFO("RedisClient init success");
	
	return 0;
}
