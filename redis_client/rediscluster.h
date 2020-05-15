#ifndef REDISCLUSTER_H
#define REDISCLUSTER_H

#include <string>
#include <map>
#include <list>
#include <vector>
#include "redisconnection.h"
#include "rwmutex.h"


using namespace std;

namespace GBDownLinker {


typedef vector<RedisConnection*> REDIS_CONNECTIONS;

class RedisCluster
{
public:
	RedisCluster();
	~RedisCluster();
	bool initConnectPool(const string& clusterIp, uint32_t clusterPort, uint32_t connectionNum, uint32_t connectTimeout, uint32_t readTimeout, const std::string& passwd="");
	bool freeConnectPool();
	bool doRedisCommand(list<RedisCmdParaInfo>& paraList, int32_t paraLen, RedisReplyInfo& replyInfo, RedisConnection::ReplyParserType parserType = RedisConnection::COMMON_PARSER);
	bool doRedisCommandOneConnection(list<RedisCmdParaInfo>& paraList, int32_t paraLen, RedisReplyInfo& replyInfo, bool release, RedisConnection** conn);
	bool releaseConnection(RedisConnection* conn);
	//for if connection not can be used, free it and create a new.
	void freeConnection(RedisConnection* conn);
	bool checkIfCanFree();
	RedisConnection* getAvailableConnection();
	RedisConnection* getUnreleasedConnection();

	bool doCommandWithParseEnhance(list < RedisCmdParaInfo >& paraList, int32_t paraLen, CommonReplyInfo& replyInfo);
	
	std::string GetClusterAddr() {
		return m_clusterIp + ":" + toStr(m_clusterPort);
	}

private:
	bool checkConnectionAlive(RedisConnection* conn);
	void showConnectionPool();

private:
	string	m_clusterIp;
	uint32_t 	m_clusterPort;
	uint32_t	m_connectionNum;
	uint32_t 	m_connectTimeout;
	uint32_t	m_readTimeout;
	std::string m_passwd;
	REDIS_CONNECTIONS m_connections;
	REDIS_CONNECTIONS m_tmpConnections;
	RWMutex m_lockAvailableConnection;
};

} // namespace GBDownLinker

#endif
