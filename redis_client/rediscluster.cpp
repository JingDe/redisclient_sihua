#include "rediscluster.h"
#include "redis_client_util.h"
#include "base_library/log.h"
#include <sstream>

namespace GBDownLinker {

#define CONNECTION_RELEASE_TIME 10

RedisCluster::RedisCluster()
{
	m_clusterIp.clear();
	m_clusterPort = 0;
	m_connectionNum = 0;
	m_connectTimeout = 0;
	m_readTimeout = 0;
	m_passwd.clear();
	m_connections.clear();
	m_tmpConnections.clear();
}

RedisCluster::~RedisCluster()
{
	freeConnectPool();
}

bool RedisCluster::initConnectPool(const string& clusterIp, uint32_t clusterPort, uint32_t connectionNum, uint32_t connectTimeout, uint32_t readTimeout, const std::string& passwd)
{
	m_clusterIp = clusterIp;
    m_clusterPort = clusterPort;
    m_connectionNum = connectionNum;
    m_connectTimeout = connectTimeout;
    m_readTimeout = readTimeout;
	m_passwd = passwd;

	std::stringstream log_msg;
	WriteGuard guard(m_lockAvailableConnection);
	for (unsigned int i = 0; i < connectionNum; i++)
	{
		RedisConnection* connection = new RedisConnection(clusterIp, clusterPort, connectTimeout, readTimeout, passwd);
		if (!connection->connect())
		{
			log_msg.str("");
			log_msg << "connect to clusterIp:" << clusterIp << " clusterPort:" << clusterPort << " failed.";
			LOG_WRITE_ERROR(log_msg.str());
			delete connection;
			goto CLEAR;
		}
		connection->m_available = true;
		connection->m_connectTime = 0;
		m_connections.push_back(connection);
	}

	log_msg.str("");
	log_msg << "initConnectPool ok, pool size is " << m_connections.size();
	LOG_WRITE_INFO(log_msg.str());
	return true;

CLEAR:
	for (size_t i = 0; i < m_connections.size(); i++)
	{
		m_connections[i]->close();
		delete m_connections[i];
		m_connections[i]=NULL;
	}
	m_connections.clear();
	return false;
}

bool RedisCluster::freeConnectPool()
{
	WriteGuard guard(m_lockAvailableConnection);
	REDIS_CONNECTIONS::iterator iter;
	for (iter = m_connections.begin(); iter != m_connections.end(); iter++)
	{
		RedisConnection* connection = (*iter);
		if (connection != NULL)
		{
			connection->close();
			delete connection;
			connection = NULL;
		}
	}
	m_connections.clear();

	for (iter = m_tmpConnections.begin(); iter != m_tmpConnections.end(); iter++)
	{
		RedisConnection* connection = (*iter);
		if (connection != NULL)
		{
			connection->close();
			delete connection;
			connection = NULL;
		}
	}
	m_tmpConnections.clear();
	return true;
}

bool RedisCluster::checkIfCanFree()
{
	WriteGuard guard(m_lockAvailableConnection);
	bool findFree = true;
	REDIS_CONNECTIONS::iterator iter;
	for (iter = m_connections.begin(); iter != m_connections.end(); iter++)
	{
		RedisConnection* conn = *iter;
		if (!conn->m_available)
		{
			findFree = false;
			break;
		}
	}
	return findFree;
}


bool RedisCluster::doCommandWithParseEnhance(list < RedisCmdParaInfo >& paraList, int32_t paraLen, CommonReplyInfo& replyInfo)
{
	bool success = false;
	RedisConnection* connection = getAvailableConnection();
	if (connection != NULL)
	{
		success = connection->doCommandWithParseEnhance(paraList, paraLen, replyInfo);
		releaseConnection(connection);
	}
	else
	{
		LOG_WRITE_INFO("no available connection, new one");
		connection = new RedisConnection(m_clusterIp, m_clusterPort, m_connectTimeout, m_readTimeout, m_passwd);
		if (connection == NULL)
			return false;
		if (!connection->connect())
		{
			delete connection;
			return false;
		}
		success = connection->doCommandWithParseEnhance(paraList, paraLen, replyInfo);
		connection->close();
		delete connection;
		connection = NULL;
	}
	return success;
}


bool RedisCluster::doRedisCommand(list < RedisCmdParaInfo >& paraList, int32_t paraLen, RedisReplyInfo& replyInfo, RedisConnection::ReplyParserType parserType)
{
	bool success = false;
	RedisConnection* connection = getAvailableConnection();
	if (connection != NULL)
	{
		success = connection->doRedisCommand(paraList, paraLen, replyInfo, parserType);
		releaseConnection(connection);
	}
	else
	{
		LOG_WRITE_INFO("no available connection, new one");
		connection = new RedisConnection(m_clusterIp, m_clusterPort, m_connectTimeout, m_readTimeout, m_passwd);
		if (connection == NULL)
			return false;
		if (!connection->connect())
		{
			delete connection;
			return false;
		}
		success = connection->doRedisCommand(paraList, paraLen, replyInfo, parserType);
		connection->close();
		delete connection;
		connection = NULL;
	}
	return success;
}

bool RedisCluster::doRedisCommandOneConnection(list < RedisCmdParaInfo >& paraList, int32_t paraLen, RedisReplyInfo& replyInfo, bool release, RedisConnection** conn)
{
	bool success = false;
	if (*conn == NULL)
	{
		*conn = getAvailableConnection();
	}
	if (*conn != NULL)
	{
		success = (*conn)->doRedisCommand(paraList, paraLen, replyInfo);
	}
	else
	{
		*conn = new RedisConnection(m_clusterIp, m_clusterPort, m_connectTimeout, m_readTimeout, m_passwd);
		if (!(*conn)->connect())
		{
			return false;
		}
		(*conn)->m_connectTime = getCurrTimeSec();
		success = (*conn)->doRedisCommand(paraList, paraLen, replyInfo);
		WriteGuard guard(m_lockAvailableConnection);
		m_tmpConnections.push_back(*conn);
	}
	if (release)
		releaseConnection(*conn);
	return success;
}

RedisConnection* RedisCluster::getUnreleasedConnection()
{
	std::stringstream log_msg;

	WriteGuard guard(m_lockAvailableConnection);
	uint32_t currentTime = getCurrTimeSec();
	REDIS_CONNECTIONS::iterator iter;
	for (iter = m_tmpConnections.begin(); iter != m_tmpConnections.end(); )
	{
		RedisConnection* conn = *iter;
		if (currentTime >= (conn->m_connectTime + CONNECTION_RELEASE_TIME))
		{
			//need close it for clean watch key.
			conn->close();
			delete conn;
			iter = m_tmpConnections.erase(iter);
		}
		else
		{
			iter++;
		}
	}

	for (iter = m_connections.begin(); iter != m_connections.end(); iter++)
	{
		RedisConnection* conn = *iter;
		if (conn->m_available)
		{
			conn->m_available = false;
			conn->m_connectTime = currentTime;
			conn->SetCanRelease(false);
			return conn;
		}
	}
	for (iter = m_connections.begin(); iter != m_connections.end(); iter++)
	{
		RedisConnection* conn = *iter;
		if (conn->CanRelease() && currentTime >= (conn->m_connectTime + CONNECTION_RELEASE_TIME))
		{
			//need close it for clean watch key.
			conn->close();
			delete conn;
			conn = NULL;
			//recreate redis connection.
			conn = new RedisConnection(m_clusterIp, m_clusterPort, m_connectTimeout, m_readTimeout, m_passwd);
			if (!conn->connect())
			{
				log_msg.str("");
				log_msg << "connect to clusterIp:" << m_clusterIp << " clusterPort:" << m_clusterPort << " failed.";
				LOG_WRITE_ERROR(log_msg.str());
				return NULL;
			}
			conn->m_available = false;
			conn->m_connectTime = currentTime;
			conn->SetCanRelease(false);
			*iter = conn;
			return conn;
		}

	}
	return NULL;
}

RedisConnection* RedisCluster::getAvailableConnection()
{
	std::stringstream log_msg;

	WriteGuard guard(m_lockAvailableConnection);
	//showConnectionPool();

	uint32_t currentTime = getCurrTimeSec();
	REDIS_CONNECTIONS::iterator iter;
	for (iter = m_tmpConnections.begin(); iter != m_tmpConnections.end(); )
	{
		RedisConnection* conn = *iter;
		if (currentTime >= (conn->m_connectTime + CONNECTION_RELEASE_TIME))
		{
			//need close it for clean watch key.
			conn->close();
			delete conn;
			iter = m_tmpConnections.erase(iter);
		}
		else
		{
			iter++;
		}
	}

	for (iter = m_connections.begin(); iter != m_connections.end(); iter++)
	{
		RedisConnection* conn = *iter;
		if (conn->m_available)
		{
			conn->m_available = false;
			conn->m_connectTime = currentTime;
//			LOG_WRITE_INFO("get available connection ok from connection pool");
			return conn;
		}
	}

	for (iter = m_connections.begin(); iter != m_connections.end(); iter++)
	{
		RedisConnection* conn = *iter;
		if (conn->CanRelease() && currentTime >= (conn->m_connectTime + CONNECTION_RELEASE_TIME))
		{
			//need close it for clean watch key.
			conn->close();
			delete conn;
			conn = NULL;
			//recreate redis connection.
			conn = new RedisConnection(m_clusterIp, m_clusterPort, m_connectTimeout, m_readTimeout, m_passwd);
			if (!conn->connect())
			{
				log_msg.str("");
				log_msg << "connect to clusterIp:" << m_clusterIp << " clusterPort:" << m_clusterPort << " failed.";
				LOG_WRITE_ERROR(log_msg.str());
				return NULL;
			}
			conn->m_available = false;
			conn->m_connectTime = currentTime;
			*iter = conn;
//			LOG_WRITE_INFO("release and reconnect ok");
			return conn;
		}
	}
//	LOG_WRITE_INFO("can not get available connection");
	return NULL;
}

void RedisCluster::showConnectionPool()
{
	std::stringstream log_msg;
	int cnt=0;
	log_msg<<"show pool size "<<m_connections.size();
	LOG_WRITE_INFO(log_msg.str());	
	REDIS_CONNECTIONS::iterator iter;
	for (iter = m_connections.begin(); iter != m_connections.end(); iter++)
	{
		if((*iter)->m_available)
		{
			log_msg.str("");
			log_msg<<"No."<<cnt<<" conn is available";
			LOG_WRITE_INFO(log_msg.str());
		}
		else
		{
			log_msg.str("");
			log_msg<<"No."<<cnt<<" conn is allocated";
			LOG_WRITE_INFO(log_msg.str());
		}
		cnt++;
	}
}

bool RedisCluster::releaseConnection(RedisConnection* conn)
{
	WriteGuard guard(m_lockAvailableConnection);
	REDIS_CONNECTIONS::iterator iter;
	for (iter = m_connections.begin(); iter != m_connections.end(); iter++)
	{
		if ((*iter) == conn)
		{
			(*iter)->m_available = true;
			//LOG_WRITE_INFO("release connection in conection pool ok");
			//showConnectionPool();
			return true;
		}
	}

	{
//		LOG_WRITE_INFO("release failed in connection pool");

		for (iter = m_tmpConnections.begin(); iter != m_tmpConnections.end(); iter++)
		{
			if ((*iter) == conn)
			{
				conn->close();
				delete conn;
				conn = NULL;
				m_tmpConnections.erase(iter);
//				LOG_WRITE_INFO("release ok in tmp connections");
				return true;
			}
		}
	}
	return false;
}

void RedisCluster::freeConnection(RedisConnection* conn)
{
	WriteGuard guard(m_lockAvailableConnection);
	uint32_t currentTime = getCurrTimeSec();
	REDIS_CONNECTIONS::iterator iter;
	for (iter = m_connections.begin(); iter != m_connections.end(); iter++)
	{
		//check connection
		if ((*iter) == conn)
		{
			conn->close();
			delete conn;
			conn = NULL;
			//recreate redis connection.
			conn = new RedisConnection(m_clusterIp, m_clusterPort, m_connectTimeout, m_readTimeout, m_passwd);
			if (!conn->connect())
			{
				return;
			}
			conn->m_available = false;
			conn->m_connectTime = currentTime;
			*iter = conn;
			return;
		}
	}
}

bool RedisCluster::checkConnectionAlive(RedisConnection* conn)
{
	WriteGuard guard(m_lockAvailableConnection);
	bool find = false;
	REDIS_CONNECTIONS::iterator iter;
	for (iter = m_connections.begin(); iter != m_connections.end(); iter++)
	{
		if ((*iter) == conn)
		{
			if ((*iter)->m_available)
			{
				(*iter)->m_available = false;
			}
			find = true;
			break;
		}
	}
	if (!find)
	{
		bool findTmp = false;
		for (iter = m_tmpConnections.begin(); iter != m_tmpConnections.end(); iter++)
		{
			if ((*iter) == conn)
			{
				findTmp = true;
				break;
			}
		}
		(void)findTmp;
	}
	return find;
}

} // namespace GBDownLinker 
