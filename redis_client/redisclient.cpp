#include "redisclient.h"
#include "redisconnection.h"
#include "redis_client_util.h"
#include "mutexlockguard.h"
#include "serialize.h"
//#include "redismonitor.h"

#include <set>
#include <sstream>

#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

namespace GBDownLinker {

std::ostream& operator<<(std::ostream& os, enum RedisClientInitResult result)
{
	switch (result)
	{
		case RedisClientInitResult::InitSuccess:
			os<<"Init Success";
			break;
		case RedisClientInitResult::RecoverableFail:
			os<<"Recoverable Fail";
			break;
		case RedisClientInitResult::UnrecoverableFail:
			os<<"Unrecoverable Fail";
			break;
	}
	return os;
}

std::ostream& operator<<(std::ostream& os, enum RedisClientStatus status)
{
    switch (status)
    {    
    case RedisClientStatus::RedisClientNormal:
        os<<"Normal";
		break;
    case RedisClientStatus::RedisClientRecoverable:
        os<<"Recoverable Exception";
		break;
    case RedisClientStatus::RedisClientUnrecoverable:
        os<<"Unrecoverable Exception";
		break;
    }    
    return os;
}


static void DefaultCallback(RedisClientStatus status)
{
	std::stringstream log_msg;
	log_msg << "RedisClient DefaultCallback: redis client status change to " << status;
	LOG_WRITE_WARNING(log_msg.str());
}

static bool EpollAdd(int epfd, int fd, int eventType)
{
	struct epoll_event event;
	event.events=eventType;
	event.data.fd=fd;
	if(::epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event)<0)
	{
		std::stringstream log_msg;
		log_msg<<"epoll_ctl ADD failed: "<<errno<<", "<<strerror(errno);
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	return true;
}

static bool EpollDel(int epfd, int fd)
{
	if(epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL)<0)
	{
		std::stringstream log_msg;
		log_msg<<"epoll_ctl DEL failed: "<<errno<<", "<<strerror(errno);
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	return true;
}

RedisClient::RedisClient()
	:m_redisMode(STAND_ALONE_OR_PROXY_MODE),
	m_connectionNum(0),
	m_connectTimeout(-1),
	m_readTimeout(-1),
	m_passwd(),
	m_connected(false),
	m_redisProxy(),
	// for CLUSER_MODE
	m_serverList(),
	m_slotMap(),
	m_rwSlotMutex(),
	m_clusterMap(),
	m_rwClusterMutex(),
	m_unusedHandlers(),
	//	m_redisMonitor(NULL),
	m_checkClusterNodesThreadId(),
	m_checkClusterNodesThreadStarted(false),
	m_checkClusterSignalQueue(),
	m_lockCheckClusterSignalQueue(),
	m_condCheckClusterSignalQueue(&m_lockCheckClusterSignalQueue),
	// END for CLUSTER_NODE
	// for SENTINEL_MODE
	m_sentinelList(),
	m_masterName(),
	m_sentinelHandlers(),
	m_rwSentinelHandlers(),
	m_initMasterAddrGot(false),
	m_initMasterAddr(),
	m_dataNodes(),
	m_masterClusterId(),
	m_rwMasterMutex(),
	m_sentinelHealthCheckThreadId(),
	m_sentinelHealthCheckThreadStarted(false),
	m_forceSentinelHealthCheckThreadExit(false),
	m_subscribeSwitchMasterThreadId(),
	m_subscribeSwitchMasterThreadRunning(false),
	m_forceSubscribeThreadExit(false),
	m_lockSubscribeConnections(),
	m_subscribeConnections(),
	m_notifySubscribeEventFd(-1),
	m_checkMasterSignalQueue(),
	m_lockCheckMasterSignalQueue(),
	m_condCheckMasterSignalQueue(&m_lockCheckMasterSignalQueue),
	// END for SENTINEL_MODE
	m_workStatus(RedisClientStatus::RedisClientNormal),
	m_callback(DefaultCallback)
{
	LOG_WRITE_INFO("construct RedisClient ok");


}

RedisClient::~RedisClient()
{
//	if (m_connected == true)
		freeRedisClient();
}

bool RedisClient::freeRedisClient()
{
//	if (m_connected == false)
//	{
//		LOG_WRITE_WARNING("why RedisClient freeRedisClient called when m_connected is false");
//		return false;
//	}
	LOG_WRITE_INFO("free redisclients");

	if (m_redisMode == STAND_ALONE_OR_PROXY_MODE)
	{
		freeRedisProxy();
	}
	else if (m_redisMode == CLUSTER_MODE)
	{
		freeRedisCluster();
//		if(m_redisMonitor)
//		{
//			m_redisMonitor->cancel();
//			delete m_redisMonitor;
//			m_redisMonitor=NULL;
//		}
	}
	else if (m_redisMode == SENTINEL_MODE)
	{
		StopSentinelThreads();
		freeSentinels();
		freeMasterSlaves();
	}

	ResetStatus();
	return true;
}

void RedisClient::ResetStatus()
{
	m_connected = false;
	m_passwd.clear();
	m_workStatus = RedisClientStatus::RedisClientNormal;
}

bool RedisClient::init(const string& serverIp, uint32_t serverPort, uint32_t connectionNum, uint32_t connectTimeout, uint32_t readTimeout, const string& passwd)
{
	RedisServerInfo server(serverIp, serverPort);
	REDIS_SERVER_LIST serverList({ server });
	return init(STAND_ALONE_OR_PROXY_MODE, serverList, "", connectionNum, connectTimeout, readTimeout, passwd);
}

bool RedisClient::init(const REDIS_SERVER_LIST& clusterList, uint32_t connectionNum, uint32_t connectTimeout, uint32_t readTimeout, const string& passwd)
{
	return init(CLUSTER_MODE, clusterList, "", connectionNum, connectTimeout, readTimeout, passwd);
}

bool RedisClient::init(const REDIS_SERVER_LIST& sentinelList, const string& masterName, uint32_t connectionNum, uint32_t connectTimeout, uint32_t readTimeout, const string& passwd)
{
	return init(SENTINEL_MODE, sentinelList, masterName, connectionNum, connectTimeout, readTimeout, passwd);
}

RedisClientInitResult RedisClient::init(RedisMode redis_mode, const REDIS_SERVER_LIST& serverList, const string& masterName, uint32_t connectionNum, uint32_t connectTimeout, uint32_t readTimeout, const string& passwd)
{
	if (m_connected == true)
	{
		LOG_WRITE_WARNING("why RedisClient init called, when m_connected is true");
		return InitSuccess;
	}

	m_redisMode = redis_mode;
	m_masterName = masterName;
	if (m_redisMode == SENTINEL_MODE)
		m_sentinelList = serverList;
	else
		m_serverList = serverList;
	m_connectionNum = connectionNum;
	m_connectTimeout = connectTimeout;
	m_readTimeout = readTimeout;
	m_passwd = passwd;

	if (m_redisMode == STAND_ALONE_OR_PROXY_MODE)
	{
		if (!initRedisProxy())
		{
			LOG_WRITE_ERROR("init redis proxy failed.");
			return UnrecoverableFail;
		}
		std::stringstream log_msg;
		log_msg << "init the redis proxy or redis server " << m_redisProxy.connectIp << ", " << m_redisProxy.connectPort << " ok";
		LOG_WRITE_INFO(log_msg.str());
	}
	else if (m_redisMode == CLUSTER_MODE)
	{
		if (!getRedisClusterNodes())
		{
			LOG_WRITE_ERROR("get redis cluster info failed.please check redis config.");
			return UnrecoverableFail;
		}

		if (!initRedisCluster())
		{
			LOG_WRITE_ERROR("init redis cluster failed.");
			return UnrecoverableFail;
		}

		if (StartCheckClusterThread() == false)
		{
			LOG_WRITE_ERROR("start thread to check cluster nodes failed");
			return UnrecoverableFail;
		}
	}
	else if (m_redisMode == SENTINEL_MODE)
	{
		// 1, start sentinel health check thread, this thread do the following work:
		// 1.1 try initially connect to sentinel nodes
		// 1.2 ckquorum
		// 1.3 sentinel get-master-address-by-name MASTER_NAME
		// 1.4 sleep, and wait do_command_threads to notify the master disconnection and awake
		if (!StartSentinelHealthCheckTask())
		{
			LOG_WRITE_ERROR("fail to start sentinel health check thread");
			return UnrecoverableFail;
		}

		// 2, start subscribe +switch-master thread, to all sentinel nodes
		if(!StartSubscribeSwitchMasterTask())
		{
			LOG_WRITE_WARNING("fail to start subscribe +switch-master thread");
			// let sentinel health check thread 1.3 do this job
		}

		// 3, connect to sentinel nodes
		if (!initSentinels())
		{
			LOG_WRITE_ERROR("init sentinels failed");
			m_connected=true;
			return RecoverableFail;
		}

		// 4, connect to data nodes
		if (!initMasterSlaves())
		{
			LOG_WRITE_ERROR("init master and slaves connection failed");
			m_connected=true;
			return RecoverableFail;
		}

		LOG_WRITE_INFO("init redis sentinels and data nodes connection ok");
	}
	else
	{
		std::stringstream log_msg;
		log_msg << "unsupported redis mode " << m_redisMode;
		LOG_WRITE_ERROR(log_msg.str());
		return UnrecoverableFail;
	}

	m_connected = true;
	return InitSuccess;
}

void RedisClient::SetCallback(const StatusChangedCallback& callback)
{
	m_callback=callback;
}

bool RedisClient::initRedisProxy()
{
	//    WriteGuard guard(m_rwProxyMutex);
	m_redisProxy.connectIp = m_serverList.front().serverIp;
	m_redisProxy.connectPort = m_serverList.front().serverPort;
	m_redisProxy.proxyId = m_redisProxy.connectIp + ":" + toStr(m_redisProxy.connectPort);
	m_redisProxy.connectionNum = m_connectionNum;
	m_redisProxy.connectTimeout = m_connectTimeout;
	m_redisProxy.readTimeout = m_readTimeout;

	if (m_redisProxy.clusterHandler != NULL)
	{
		LOG_WRITE_WARNING("why m_redisProxy.clusterHandler not NULL when initRedisProxy");
		freeRedisProxy();
	}
	m_redisProxy.clusterHandler = new RedisCluster();
	if (m_redisProxy.clusterHandler == NULL)
	{
		return false;
	}
	if (!m_redisProxy.clusterHandler->initConnectPool(m_redisProxy.connectIp, m_redisProxy.connectPort, m_redisProxy.connectionNum, m_redisProxy.connectTimeout, m_redisProxy.readTimeout))
	{
		std::stringstream log_msg;
		log_msg << "init redis-server:[" << m_redisProxy.proxyId << "] connect pool failed.";
		LOG_WRITE_ERROR(log_msg.str());
		delete m_redisProxy.clusterHandler;
		m_redisProxy.clusterHandler = NULL;
		return false;
	}

	std::stringstream log_msg;
	log_msg << "init redis-server:[" << m_redisProxy.proxyId << "] connect pool ok.";
	LOG_WRITE_INFO(log_msg.str());

	if (m_passwd.empty() == false)
	{
		AuthPasswd(m_passwd, m_redisProxy.clusterHandler);
	}
	return true;
}


bool RedisClient::getRedisClusterNodes()
{
	assert(m_redisMode == CLUSTER_MODE);
	bool success = false;
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("cluster", 7, paraList);
	fillCommandPara("nodes", 5, paraList);
	paraLen += 30;
	REDIS_SERVER_LIST::iterator iter;
	for (iter = m_serverList.begin(); iter != m_serverList.end(); iter++)
	{
		RedisServerInfo serverInfo = (*iter);

		RedisConnection* connection = new RedisConnection(serverInfo.serverIp, serverInfo.serverPort, m_connectTimeout, m_readTimeout, "");
		if (!connection->connect())
		{
			std::stringstream log_msg;
			log_msg << "connect to serverIp:[" << serverInfo.serverIp << "] serverPort:[" << serverInfo.serverPort << "] failed.";
			LOG_WRITE_WARNING(log_msg.str());
			delete connection;
			continue;
		}
		//send cluster nodes.
		RedisReplyInfo replyInfo;
		if (!connection->doRedisCommand(paraList, paraLen, replyInfo))
		{
			LOG_WRITE_WARNING("do get cluster nodes failed.");
			connection->close();
			delete connection;
			freeReplyInfo(replyInfo);
			freeCommandList(paraList);
			continue;
		}
		if (replyInfo.replyType == REDIS_REPLY_ERROR)
		{
			std::stringstream log_msg;
			log_msg << "recv redis error response:[" << replyInfo.resultString << "].";
			LOG_WRITE_ERROR(log_msg.str());
			connection->close();
			delete connection;
			freeReplyInfo(replyInfo);
			freeCommandList(paraList);
			return false;
		}
		if (replyInfo.replyType != REDIS_REPLY_STRING)
		{
			std::stringstream log_msg;
			log_msg << "recv redis wrong reply type:[" << replyInfo.replyType << "].";
			LOG_WRITE_ERROR(log_msg.str());
			connection->close();
			delete connection;
			freeReplyInfo(replyInfo);
			freeCommandList(paraList);
			return false;
		}
		//check if master,slave parameter may be not newest.
//		list<ReplyArrayInfo>::iterator arrayIter = replyInfo.arrayList.begin();
//		string str = (*arrayIter).arrayValue;
//		if (str.find("myself,slave") != string::npos)
//		{
//			freeReplyInfo(replyInfo);
//			connection->close();
//			delete connection;
//			continue;
//		}
		REDIS_CLUSTER_MAP clusterMap;
		if (!parseClusterInfo(replyInfo, clusterMap))
		{
			LOG_WRITE_ERROR("parse cluster info from redis reply failed.");
			connection->close();
			delete connection;
			freeReplyInfo(replyInfo);
			freeCommandList(paraList);
			return false;
		}
		{
			WriteGuard guard(m_rwClusterMutex);
			m_clusterMap = clusterMap;
		}
		freeReplyInfo(replyInfo);
		connection->close();
		delete connection;
		success = true;
		break;
	}
	freeCommandList(paraList);
	return success;
}

bool RedisClient::initRedisCluster()
{
	WriteGuard guard(m_rwClusterMutex);
	REDIS_CLUSTER_MAP::iterator clusterIter;
	for (clusterIter = m_clusterMap.begin(); clusterIter != m_clusterMap.end(); clusterIter++)
	{
		((*clusterIter).second).clusterHandler = new RedisCluster();
		if (!((*clusterIter).second).clusterHandler->initConnectPool(((*clusterIter).second).connectIp, ((*clusterIter).second).connectPort, ((*clusterIter).second).connectionNum, ((*clusterIter).second).connectTimeout, ((*clusterIter).second).readTimeout))
		{
			std::stringstream log_msg;
			log_msg << "init cluster:[" << (*clusterIter).first << "] connect pool failed.";
			LOG_WRITE_ERROR(log_msg.str());
			delete ((*clusterIter).second).clusterHandler;
			((*clusterIter).second).clusterHandler = NULL;
			return false;
		}
		//fill slot map
		if (((*clusterIter).second).isMaster)
		{
			WriteGuard guard(m_rwSlotMutex);
			map<uint16_t, uint16_t>::iterator iter;
			for (iter = ((*clusterIter).second).slotMap.begin(); iter != ((*clusterIter).second).slotMap.end(); iter++)
			{
				uint16_t startSlotNum = (*iter).first;
				uint16_t stopSlotNum = (*iter).second;
				for (int i = startSlotNum; i <= stopSlotNum; i++)
				{
					m_slotMap[i] = (*clusterIter).first;
				}
			}
		}
		if (m_passwd.empty() == false)
		{
			AuthPasswd(m_passwd, ((*clusterIter).second).clusterHandler);
		}
	}
	return true;
}

bool RedisClient::initSentinels()
{
	std::stringstream log_msg;
	m_initMasterAddrGot = false;
	int aliveSentinel = 0;
	bool switchMasterSubsribed = false;
	WriteGuard guard(m_rwSentinelHandlers);
	for (REDIS_SERVER_LIST::const_iterator it = m_sentinelList.begin(); it != m_sentinelList.end(); ++it)
	{
		RedisProxyInfo sentinelHandler;
		sentinelHandler.connectIp = it->serverIp;
		sentinelHandler.connectPort = it->serverPort;
		sentinelHandler.proxyId = it->serverIp + ":" + toStr(sentinelHandler.connectPort);
		sentinelHandler.isAlived = false;
		sentinelHandler.subscribed = false;
		sentinelHandler.clusterHandler = new RedisCluster;
		if (sentinelHandler.clusterHandler == NULL)
		{
			LOG_WRITE_ERROR("new RedisCluster failed");
		}
		else
		{
			// one for control and check, like ckquorum; 
			// one for subscribe +switch-master;
			int connection_num_to_sentinel = 2;
			if (!sentinelHandler.clusterHandler->initConnectPool(sentinelHandler.connectIp, sentinelHandler.connectPort, connection_num_to_sentinel, m_connectTimeout, m_readTimeout))
			{
				log_msg.str("");
				log_msg << "init sentinel:[" << sentinelHandler.proxyId << "] connect pool failed.";
				LOG_WRITE_ERROR(log_msg.str());
			}
			else
			{
				sentinelHandler.isAlived = true;
				aliveSentinel++;
			}
		}

		m_sentinelHandlers[sentinelHandler.proxyId] = sentinelHandler;

		if (!sentinelHandler.isAlived)
			continue;

		if(!NotifyToSubscribeSwitchMaster(sentinelHandler.clusterHandler))
		{
			log_msg.str("");
			log_msg << "sentinel " << sentinelHandler.proxyId << " subscribe +switch-master failed";
			LOG_WRITE_ERROR(log_msg.str());
		}
		else
		{
			switchMasterSubsribed = true;
			m_sentinelHandlers[sentinelHandler.proxyId].subscribed = true;
		}

		if (!m_initMasterAddrGot)
		{
			if (SentinelGetMasterAddrByName(sentinelHandler.clusterHandler, m_initMasterAddr)
				&& CheckMasterRole(m_initMasterAddr))
			{
				std::stringstream log_msg;
				log_msg<<"got init master addr: "<<m_initMasterAddr.serverIp<<", "<<m_initMasterAddr.serverPort;
				LOG_WRITE_INFO(log_msg.str());
				m_initMasterAddrGot = true;
			}
		}
	}
	log_msg.str("");
	log_msg << "initial config sentinel count is " << m_sentinelList.size() << ", connected sentinel is " << aliveSentinel;
	LOG_WRITE_INFO(log_msg.str());

	if (!m_initMasterAddrGot)
	{
		LOG_WRITE_ERROR("get master addr failed");
		return false;
	}

	if (!switchMasterSubsribed)
	{
		LOG_WRITE_ERROR("subscribe +switch-master failed");
//		return false;
	}

	return true;
}

bool RedisClient::CheckMasterRole(const RedisServerInfo& masterAddr)
{
	return true;
}

bool RedisClient::StartSentinelHealthCheckTask()
{
	if (m_sentinelHealthCheckThreadStarted)
		return true;
	m_forceSentinelHealthCheckThreadExit = false;
	int ret = pthread_create(&m_sentinelHealthCheckThreadId, NULL, SentinelHealthCheckTask, this);
	if (ret)
	{
		LOG_WRITE_ERROR("start health check thread failed");
		return false;
	}
	m_sentinelHealthCheckThreadStarted = true;;
	return true;
}


// try to connect to sentinel nodes in health check thread the first time, when initSentinels failed before
bool RedisClient::SentinelReinit(SentinelStats& stats)
{
	bool sentinel_reinit_work_all_done = true;

	stats.sentinelCount=m_sentinelHandlers.size();
	stats.connectedCount=0;
	stats.subscribedCount=0;

	WriteGuard guard(m_rwSentinelHandlers);
	for (map<string, RedisProxyInfo>::iterator it = m_sentinelHandlers.begin(); it != m_sentinelHandlers.end(); ++it)
	{
		if (it->second.isAlived == false)
		{
			if (it->second.clusterHandler == NULL)
				it->second.clusterHandler = new RedisCluster;
			if (it->second.clusterHandler == NULL)
			{
				LOG_WRITE_ERROR("new RedisCluster failed");
				sentinel_reinit_work_all_done = false;
			}
			else
			{
				int connection_num_to_sentinel = 2;
				if (!it->second.clusterHandler->initConnectPool(it->second.connectIp, it->second.connectPort, connection_num_to_sentinel, m_connectTimeout, m_readTimeout))
				{
					std::stringstream log_msg;
					log_msg << "init sentinel:[" << it->second.proxyId << "] connect pool failed.";
					LOG_WRITE_ERROR(log_msg.str());
					sentinel_reinit_work_all_done = false;
				}
				else
				{
					it->second.isAlived = true;
					std::stringstream log_msg;
					log_msg << "sentinel now online: " << it->second.connectIp << ", " << it->second.connectPort;
					LOG_WRITE_INFO(log_msg.str());
				}
			}
		}

		if (it->second.isAlived  &&  !it->second.subscribed)
		{
			if(!NotifyToSubscribeSwitchMaster(it->second.clusterHandler))
			{
				std::stringstream log_msg;
				log_msg << "sentinel " << it->second.proxyId << " subscribe +switch-master failed";
				LOG_WRITE_ERROR(log_msg.str());
				sentinel_reinit_work_all_done = false;
			}
			else
			{
				it->second.subscribed = true;
			}
		}

		if(it->second.isAlived)		
			stats.connectedCount++;
		if(it->second.subscribed)
			stats.subscribedCount++;
	}

	return sentinel_reinit_work_all_done;
}


void* RedisClient::SentinelHealthCheckTask(void* arg)
{
	LOG_WRITE_INFO("sentinel health check thread start");

	RedisClient* client = (RedisClient*)arg;

	bool sentinel_reinit_work_all_done = false;

	while (!client->m_forceSentinelHealthCheckThreadExit)
	{
		if (sentinel_reinit_work_all_done == false)
		{
			SentinelStats stats;
			sentinel_reinit_work_all_done = client->SentinelReinit(stats);

			if(sentinel_reinit_work_all_done==false)
			{
				std::stringstream log_msg;
				log_msg<<"sentinel stats: "<<stats.sentinelCount<<" sentinels, "<<stats.connectedCount<<" connected, "<<stats.subscribedCount<<" subscribed";
				LOG_WRITE_WARNING(log_msg.str());
			}
		}

		if (client->CheckSentinelCkquorum() == false)
		{
			if(client->m_workStatus == RedisClientStatus::RedisClientNormal)
				(client->m_callback)(RedisClientStatus::RedisClientRecoverable);

			client->m_workStatus = RedisClientStatus::RedisClientRecoverable;
			sleep(20);
			continue;
		}

		DoRedisCmdResultType result = client->DoConnectMasterNode();
		if( result == DoRedisCmdResultType::NoAuth)
		{
			std::stringstream log_msg;
			log_msg<<"auth failed, current passwd is "<< client->m_passwd <<", Unrecoverable now";
			LOG_WRITE_ERROR(log_msg.str());

			client->m_workStatus=RedisClientStatus::RedisClientUnrecoverable;

			(client->m_callback)(RedisClientStatus::RedisClientUnrecoverable);
			break;
		}
		else if(result != DoRedisCmdResultType::Success)
		{
			if(client->m_workStatus == RedisClientStatus::RedisClientNormal)
				(client->m_callback)(RedisClientStatus::RedisClientRecoverable);

			client->m_workStatus=RedisClientStatus::RedisClientRecoverable;

			sleep(20);
			continue;
		}
		else
		{
			if(client->m_workStatus != RedisClientStatus::RedisClientNormal)
				(client->m_callback)(RedisClientStatus::RedisClientNormal);

			client->m_workStatus=RedisClientStatus::RedisClientNormal;
		}

		assert(client->m_workStatus == RedisClientStatus::RedisClientNormal);

		{
			MutexLockGuard guard(&(client->m_lockCheckMasterSignalQueue));
			if (client->m_checkMasterSignalQueue.empty())
			{
				if(client->m_forceSentinelHealthCheckThreadExit)
                    break;
				client->m_condCheckMasterSignalQueue.WaitForSeconds(40);
			}
			if(client->m_forceSentinelHealthCheckThreadExit)
                break;
			if (!client->m_checkMasterSignalQueue.empty()) // notified and remove the notification
			{
				client->m_checkMasterSignalQueue=std::move(std::queue<int>());
			} 
		}

	}

	LOG_WRITE_INFO("sentinel health check task exit");
	client->m_sentinelHealthCheckThreadStarted = false;
	return 0;
}

bool RedisClient::DoCkquorum(RedisCluster* cluster)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	string cmd1 = "sentinel";
	fillCommandPara(cmd1.c_str(), cmd1.size(), paraList);
	paraLen += cmd1.size() + 10;
	string cmd2 = "ckquorum";
	fillCommandPara(cmd2.c_str(), cmd2.size(), paraList);
	paraLen += cmd2.size() + 10;
	fillCommandPara(m_masterName.c_str(), m_masterName.length(), paraList);
	paraLen += m_masterName.length() + 10;

	RedisReplyInfo replyInfo;
	bool success = cluster->doRedisCommand(paraList, paraLen, replyInfo);
	freeCommandList(paraList);
	if (!success)
	{
//		std::stringstream log_msg;
//		log_msg<<"sentinel ckquorum failed: " << cluster->GetClusterAddr();
//		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	if (!ParseSentinelCkquorumReply(replyInfo))
	{
		freeReplyInfo(replyInfo);
		return false;
	}
	else
	{
		freeReplyInfo(replyInfo);
		return true;
	}
}

bool RedisClient::ParseSentinelCkquorumReply(const RedisReplyInfo& replyInfo)
{
//	std::stringstream log_msg;
//	log_msg << "sentinel ckquorum command has replyType " << replyInfo.replyType << ", resultString " << replyInfo.resultString << ", intValue " << replyInfo.intValue;
//	LOG_WRITE_INFO(log_msg.str());

	if (replyInfo.replyType == REDIS_REPLY_ERROR)
	{
		LOG_WRITE_ERROR("ckquorum command reply error");
		return false;
	}

	if (replyInfo.replyType != REDIS_REPLY_STATUS)
	{
		std::stringstream log_msg;
		log_msg << "ckquorum failed, redis response:[" << replyInfo.resultString << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	return true;
}

bool RedisClient::CheckSentinelCkquorum()
{
	ReadGuard guard(m_rwSentinelHandlers);
	for (map<string, RedisProxyInfo>::iterator it = m_sentinelHandlers.begin(); it != m_sentinelHandlers.end(); ++it)
	{
		if (it->second.isAlived && DoCkquorum(it->second.clusterHandler))
		{
			return true;
		}
	}
	LOG_WRITE_ERROR("ckquorum all failed");
	return false;
}


bool RedisClient::SubscribeSwitchMaster(RedisConnection* con)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	string cmd1 = "subscribe";
	fillCommandPara(cmd1.c_str(), cmd1.size(), paraList);
	paraLen += cmd1.size() + 10;
	string cmd2 = "+switch-master";
	fillCommandPara(cmd2.c_str(), cmd2.size(), paraList);
	paraLen += cmd2.size() + 10;

	RedisReplyInfo replyInfo;
	bool success = con->doRedisCommand(paraList, paraLen, replyInfo);
	if (!success)
	{
		LOG_WRITE_ERROR("do cmd subscribe +switch-master failed");
		return false;
	}

	if (!ParseSubsribeSwitchMasterReply(replyInfo))
	{
		LOG_WRITE_ERROR("subscribe +switch-master failed");
		return false;
	}

	return true;
}


bool RedisClient::NotifyToSubscribeSwitchMaster(RedisCluster* cluster)
{
	if(!m_subscribeSwitchMasterThreadRunning)
	{
		return true;
	}
	
	RedisConnection* con = cluster->getUnreleasedConnection();
	if (con == NULL)
	{
		LOG_WRITE_ERROR("cannot acquire a redis connection to subscribe +switch-master");
		return false;
	}

	{
	MutexLockGuard guard(&m_lockSubscribeConnections);
	m_subscribeConnections.push_back(con);
	}

	uint64_t msg=1;
	int ret=0;
	do{
		ret=::write(m_notifySubscribeEventFd, &msg, sizeof(msg));
	} while(ret<0  &&  errno==EAGAIN);

	if(ret<0)
	{
		std::stringstream log_msg;
		log_msg<<"write eventfd failed: "<<errno<<", "<<::strerror(errno);
		LOG_WRITE_ERROR(log_msg.str());

		// TODO whether or not remove con from m_subscribeConnections and release
//		RemoveSubscribeConnection(con);
//		cluster->releaseConnection(con);		
		return false;
	}
	
	return true;
}

bool RedisClient::StartSubscribeSwitchMasterTask()
{
	if (m_subscribeSwitchMasterThreadRunning)
		return true;

	m_notifySubscribeEventFd=::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if(m_notifySubscribeEventFd<0)
	{
		std::stringstream log_msg;
		log_msg<<"eventfd failed: "<<errno<<", "<<::strerror(errno);
		LOG_WRITE_ERROR(log_msg.str());

		return false;
	}
	
	m_forceSentinelHealthCheckThreadExit = false;
	int ret = pthread_create(&m_subscribeSwitchMasterThreadId, NULL, SubscribeSwitchMasterThreadWork, this);
	if (ret)
	{
		std::stringstream log_msg;
		log_msg<<"start subscribe +switch-master thread failed: "<<errno<<", "<<::strerror(errno);
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	m_subscribeSwitchMasterThreadRunning = true;
	
	return true;

}


// 订阅所有sentinel节点switch-master频道
void* RedisClient::SubscribeSwitchMasterThreadWork(void* arg)
{
	RedisClient* client=(RedisClient*)arg;

	int sentinelMaxCount=6;
	int epollSize=sentinelMaxCount+1;
	int epFd=::epoll_create(epollSize);
	if(epFd<0)
	{
		std::stringstream log_msg;
		log_msg<<"epoll_create failed: "<<errno<<", "<<::strerror(errno);
		LOG_WRITE_ERROR(log_msg.str());

		client->m_subscribeSwitchMasterThreadRunning=false;
		return (void*)0;
	}

	if(!EpollAdd(epFd, client->m_notifySubscribeEventFd, EPOLLIN))
	{
		std::stringstream log_msg;
		log_msg<<"epoll_ctl add subscribe eventfd failed: "<<errno<<::strerror(errno);

		::close(epFd);
		client->m_subscribeSwitchMasterThreadRunning=false;
		return (void*)0;
	}

	int waitMillisecs=60;
	std::map<int, RedisConnection*> subscribeFdMap; // sockfd to RedisConnection*, to handle disconnection
	std::set<RedisConnection*> disconnectedConnections; 
	struct epoll_event epollEvents[epollSize];
	
	while (!client->m_forceSubscribeThreadExit)
	{
		// re-connect and subscribe and epoll_add
		for(std::set<RedisConnection*>::iterator it=disconnectedConnections.begin(); it!=disconnectedConnections.end(); )
		{
			RedisConnection* disconnectedCon=*it;
			if(!disconnectedCon->connect())
			{
				it++;
				continue;
			}
						
			if(!client->SubscribeSwitchMaster(disconnectedCon))
			{
				std::stringstream log_msg;
				log_msg<<"subscribe +switch-master for connection "<<disconnectedCon->toString()<<" failed";
				LOG_WRITE_ERROR(log_msg.str());
				disconnectedConnections.erase(it++);
				continue;
			}
			if(!EpollAdd(epFd, disconnectedCon->GetSockfd(), EPOLLIN | EPOLLRDHUP))
			{
				std::stringstream log_msg;
				log_msg<<"subscribe succeed, but epoll_add failed for "<<disconnectedCon->toString();
				LOG_WRITE_ERROR(log_msg.str());
				disconnectedConnections.erase(it++);
				continue;
			}
			subscribeFdMap[disconnectedCon->GetSockfd()]=disconnectedCon;
			disconnectedConnections.erase(it++);
		}
	
	
		int nevents = ::epoll_wait(epFd, epollEvents, epollSize, waitMillisecs);
		if(nevents<0)
		{
			if(errno==EINTR)
			{
				continue;
			}
			else
			{
				std::stringstream log_msg;
				log_msg<<"epoll_wait failed: "<<errno<<", "<<::strerror(errno);
				LOG_WRITE_ERROR(log_msg.str());
				// TODO re-epoll_create
				break;
			}
		}

		for(int i=0; i<nevents; ++i)
		{
			int fd=epollEvents[i].data.fd;

			// eventfd event
			if(fd==client->m_notifySubscribeEventFd)
			{				
				std::vector<RedisConnection*> connections;
				{
				MutexLockGuard guard(&client->m_lockSubscribeConnections);					
				connections.swap(client->m_subscribeConnections);
				}

				for(size_t i=0; i<connections.size(); i++)
				{
					if(!client->SubscribeSwitchMaster(connections[i]))
					{
						std::stringstream log_msg;
						log_msg<<"subscribe +switch-master for connection "<<connections[i]->toString()<<" failed";
						LOG_WRITE_ERROR(log_msg.str());
						// TODO
						continue;
					}
					if(!EpollAdd(epFd, connections[i]->GetSockfd(), EPOLLIN | EPOLLRDHUP))
					{
						std::stringstream log_msg;
						log_msg<<"subscribe succeed, but epoll_add failed for "<<connections[i]->toString();
						LOG_WRITE_ERROR(log_msg.str());
						connections[i]->close();
						continue;
					}
					subscribeFdMap[connections[i]->GetSockfd()]=connections[i];
				}
			}
			// sockfd event
			else
			{
				std::map<int, RedisConnection*>::iterator it=subscribeFdMap.find(fd);
				if(it==subscribeFdMap.end())
				{
					std::stringstream log_msg;
					log_msg<<"unknown sockfd event happended";
					LOG_WRITE_ERROR(log_msg.str());
					EpollDel(epFd, fd);
					continue;
				}
				
				RedisConnection* con=it->second;

				// disconnected
				if(epollEvents[i].events  &  EPOLLRDHUP)
				{
					std::stringstream log_msg;
					log_msg<<"sentinel "<<con->toString()<<" disconnected";
					LOG_WRITE_WARNING(log_msg.str());

					EpollDel(epFd, fd);
					subscribeFdMap.erase(it);
					
					con->close();
					disconnectedConnections.insert(con);
					continue;
				}
				
				// +switch-master message		
				RedisReplyInfo replyInfo;
				if(con->recv(replyInfo)<0)
				{
					client->freeReplyInfo(replyInfo);
					std::stringstream log_msg;
					log_msg<<"sentinel "<<con->toString()<<" recv +switch-master message failed";
					LOG_WRITE_ERROR(log_msg.str());	
					continue;
				}

				RedisServerInfo masterAddr;
				if (!client->ParseSwithMasterMessage(replyInfo, masterAddr))
				{
					LOG_WRITE_ERROR("get new master failed");
					client->freeReplyInfo(replyInfo);
					continue;
				}
				client->freeReplyInfo(replyInfo);

				client->DoSwitchMaster(masterAddr);
			}
		}

	}

	::close(epFd);
	LOG_WRITE_INFO("subscribe +switch-master thread exits now");
	client->m_subscribeSwitchMasterThreadRunning=false;
	return (void*)0;
}

//void* RedisClient::SubscribeSwitchMasterThreadWork(void* arg)
//{
//	SwitchMasterThreadArgType* tmp = (SwitchMasterThreadArgType*)arg;
//
//    RedisClient* client = tmp->client;
//    RedisConnection* con = tmp->con;
//    std::stringstream log_msg;
//    log_msg << "subscribe +switch-master thread start, sentinel addr: " << con->GetServerIp() << ", " << con->GetServerPort();
//    LOG_WRITE_INFO(log_msg.str());
//
//	bool subscribed=false;
//	bool listened=false;
//	int handler;
//	while (!client->m_forceSubscribeThreadExit)
//	{
//		ConnectionStatus connectionStatus=con->CheckConnected();
//
//		if(connectionStatus!=ConnectionStatus::Connected)
//		{
//			subscribed=false;
//			::close(handler);
//			listened=false;
//		}
//
//		if(connectionStatus==ConnectionStatus::Disconnected)
//		{
//			LOG_WRITE_ERROR("sentinel disconnected, try later");
//			sleep(5);
//			continue;
//		}
//
//		if(subscribed==false  &&  !client->SubscribeSwitchMaster(con))
//		{
//			LOG_WRITE_ERROR("subscribe +switch-master failed");
//			continue;
//		}
//		
//		subscribed=true;	
//		if(listened==false)
//		{
//			if (!con->ListenMessage(handler))
//			{
//				LOG_WRITE_ERROR("fail to listen +switch-master mesages");
//				continue;
//			}
//			else
//			{
//				listened=true;
//			}
//		}
//		
//		//LOG_WRITE_INFO("waiting +switch-master channel message now ...");
//		
//		RedisReplyInfo replyInfo;
//		WaitReadEventResult result=con->WaitMessage(handler, 3*1000);
//		if (result==WaitReadEventResult::Readable)
//		{
//			con->recv(replyInfo);
//		}
//		else if(result==WaitReadEventResult::Timeout)
//		{
//			continue;
//		}
//		else if(result==WaitReadEventResult::Disconnected)
//		{
//			LOG_WRITE_ERROR("redis disconnected when wating for +switch-master messages");
//			subscribed=false;
//			::close(handler);
//			listened=false;
//			continue;
//		}
//		else if(result==WaitReadEventResult::InternalError)
//		{
//			// TODO
//			LOG_WRITE_ERROR("wait message encounter internal error");
//			close(handler);
//			listened=false;
//			continue;
//		}
//
//		RedisServerInfo masterAddr;
//		if (!client->ParseSwithMasterMessage(replyInfo, masterAddr))
//		{
//			LOG_WRITE_ERROR("get new master failed");
//			client->freeReplyInfo(replyInfo);
//			continue;
//		}
//		client->freeReplyInfo(replyInfo);
//
//		client->DoSwitchMaster(masterAddr); // TODO 
//	}
//
//	if(listened)
//		con->StopListen(handler); // ::close(handler);
//
//	LOG_WRITE_INFO("subscribe +switch-master thread exit");
//	return NULL;
//}

bool RedisClient::ParseSwithMasterMessage(const RedisReplyInfo& replyInfo, RedisServerInfo& masterAddr)
{
	std::stringstream log_msg;
	log_msg << "channel +switch-master has message, replyType " << replyInfo.replyType << ", resultString " << replyInfo.resultString << ", intValue " << replyInfo.intValue;
	LOG_WRITE_INFO(log_msg.str());

	if (replyInfo.replyType == REDIS_REPLY_ERROR)
	{
		LOG_WRITE_ERROR("get error reply.");
		return false;
	}

	if (replyInfo.replyType != REDIS_REPLY_ARRAY)
	{
		std::stringstream log_msg;
		log_msg << "get non-array reply: " << replyInfo.resultString;
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	if (replyInfo.arrayList.size() != 3)
	{
		return false;
	}
	int i = 0;
	list<ReplyArrayInfo>::const_iterator arrayIter;
	for (arrayIter = replyInfo.arrayList.begin(); arrayIter != replyInfo.arrayList.end(); arrayIter++, i++)
	{
		//		std::stringstream log_msg;
		//		log_msg<<"arrayList has replyType "<<(*arrayIter).replyType<<", arrayValue "<<arrayIter->arrayValue<<", arrayLen "<<arrayIter->arrayLen;
		//		LOG_WRITE_INFO(log_msg.str());

		if ((*arrayIter).replyType == REDIS_REPLY_STRING)
		{
			string message = (*arrayIter).arrayValue;

			//  "mymaster 192.168.12.59 7100 192.168.12.59 7102"
			if (i == 2)
			{
				string::size_type pos = message.rfind(' ');
				if (pos == string::npos)
					return false;
				masterAddr.serverPort = atoi(message.substr(pos + 1).c_str());

				string::size_type pos2 = pos - 1;
				pos = message.rfind(' ', pos2);
				if (pos == string::npos)
					return false;
				masterAddr.serverIp = message.substr(pos + 1, pos2 - pos);

				std::stringstream log_msg;
				log_msg << "get new master addr: " << masterAddr.serverIp << ", " << masterAddr.serverPort;
				LOG_WRITE_INFO(log_msg.str());
				return true;
			}
		}
	}
	return false;
}

bool RedisClient::ParseSubsribeSwitchMasterReply(const RedisReplyInfo& replyInfo)
{
	std::stringstream log_msg;
	log_msg << "subscribe +switch-master has replyType " << replyInfo.replyType << ", resultString " << replyInfo.resultString << ", intValue " << replyInfo.intValue;
	LOG_WRITE_INFO(log_msg.str());

	if (replyInfo.replyType == REDIS_REPLY_ERROR)
	{
		LOG_WRITE_ERROR("get error reply.");
		return false;
	}

	if (replyInfo.replyType != REDIS_REPLY_ARRAY)
	{
		std::stringstream log_msg;
		log_msg << "get non-array reply: " << replyInfo.resultString;
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	if (replyInfo.arrayList.size() != 3)
	{
		std::stringstream log_msg;
		log_msg<<"wrong array size "<<replyInfo.arrayList.size();
		LOG_WRITE_ERROR(log_msg.str());	
		return false;
	}
	int i = 0;
	list<ReplyArrayInfo>::const_iterator arrayIter;
	for (arrayIter = replyInfo.arrayList.begin(); arrayIter != replyInfo.arrayList.end(); arrayIter++, i++)
	{
		//		std::stringstream log_msg;
		//		log_msg << "arrayList has replyType " << (*arrayIter).replyType << ", arrayValue " << arrayIter->arrayValue << ", arrayLen " << arrayIter->arrayLen;
		//		LOG_WRITE_INFO(log_msg.str());

		if ((*arrayIter).replyType == REDIS_REPLY_STRING)
		{
			string message = (*arrayIter).arrayValue;

			if (i == 2 && message != ":1")
			{
				std::stringstream log_msg;
				log_msg << "subscribe +switch-master return error: " << message;
				LOG_WRITE_ERROR(log_msg.str());
				return false;
			}
		}
	}
	return true;
}

DoRedisCmdResultType RedisClient::DoSwitchMaster(const RedisServerInfo& masterAddr)
{
	WriteGuard guard(m_rwMasterMutex);
	std::string masterClusterId = masterAddr.serverIp + ":" + toStr(masterAddr.serverPort);
	std::stringstream log_msg;

	if(m_masterClusterId==masterClusterId)
	{
		RedisProxyInfo& masterNode = m_dataNodes[m_masterClusterId]; 

		DoRedisCmdResultType result = CheckPingPong(masterNode.clusterHandler);
		return result;
	}

	log_msg.str("");
	log_msg<<"detect redis master changed from "<<m_masterClusterId<<" to "<<masterClusterId;
	LOG_WRITE_WARNING(log_msg.str());

	m_masterClusterId=masterClusterId;

	if (m_dataNodes.count(m_masterClusterId) == 0)
	{
		log_msg.str("");
		log_msg << "no info of new master: " << m_masterClusterId;
		LOG_WRITE_WARNING(log_msg.str());

		m_dataNodes[m_masterClusterId].connectIp = masterAddr.serverIp;
		m_dataNodes[m_masterClusterId].connectPort = masterAddr.serverPort;
		m_dataNodes[m_masterClusterId].proxyId = m_masterClusterId;
		m_dataNodes[m_masterClusterId].isAlived = false;
		m_dataNodes[m_masterClusterId].clusterHandler = NULL;
	}

	RedisProxyInfo& masterNode = m_dataNodes[m_masterClusterId];
	if (masterNode.isAlived)
	{
		if (m_passwd.empty() == false)
		{    
			if(!AuthPasswd(m_passwd, masterNode.clusterHandler))
			{    
				return DoRedisCmdResultType::NoAuth;
			}    
		}  

		DoRedisCmdResultType result = CheckPingPong(masterNode.clusterHandler);
		return result;
	}

	log_msg.str("");
	log_msg << "master " << m_masterClusterId << " not connected";
	LOG_WRITE_WARNING(log_msg.str());

	if (masterNode.clusterHandler == NULL)
	{
		masterNode.clusterHandler = new RedisCluster();
		if (masterNode.clusterHandler == NULL)
		{
			LOG_WRITE_ERROR("new clusterHandler failed");
			return DoRedisCmdResultType::InternalError;
		}
	}

	if (!masterNode.clusterHandler->initConnectPool(masterNode.connectIp, masterNode.connectPort, m_connectionNum, m_connectTimeout, m_readTimeout, m_passwd))
	{
		LOG_WRITE_ERROR("init master connection pool failed");
		return DoRedisCmdResultType::Disconnected;
	}
	masterNode.isAlived = true;

	if (m_passwd.empty() == false)
	{
		if(!AuthPasswd(m_passwd, masterNode.clusterHandler))
		{
			return DoRedisCmdResultType::NoAuth;
		}
	}
	LOG_WRITE_INFO("master switched success");
	return DoRedisCmdResultType::Success;
}

DoRedisCmdResultType RedisClient::CheckPingPong(RedisCluster* cluster)
{
	if(cluster==NULL)
		return DoRedisCmdResultType::Disconnected;
    list<RedisCmdParaInfo> paraList;                                                                   
    int32_t paraLen = 0; 
    string cmd = "ping";
    fillCommandPara(cmd.c_str(), cmd.size(), paraList, false);
    paraLen += cmd.size() + 10;

    RedisReplyInfo replyInfo;
    bool success = cluster->doRedisCommand(paraList, paraLen, replyInfo);
	freeCommandList(paraList);

    if (!success)
    {
        LOG_WRITE_ERROR("do ping command failed");
        return DoRedisCmdResultType::Disconnected;
    }

	DoRedisCmdResultType result = ParsePingReply(replyInfo);
    freeReplyInfo(replyInfo);
    return result;
}

DoRedisCmdResultType RedisClient::ParsePingReply(const RedisReplyInfo& replyInfo)
{
    if (replyInfo.replyType == REDIS_REPLY_ERROR)
    {
		std::string noauthMsg="NOAUTH";
        int minLen=std::min(replyInfo.resultString.size(), noauthMsg.size());
        if(strncmp(replyInfo.resultString.c_str(), noauthMsg.c_str(), minLen)==0)
        {    
            LOG_WRITE_WARNING("ping get NOAUTH failure");
			return DoRedisCmdResultType::NoAuth;
        }  
		else
		{
			LOG_WRITE_ERROR("ping command reply error");
			return DoRedisCmdResultType::Fail;
		}
    }
    if (replyInfo.replyType != REDIS_REPLY_STATUS)
    {
        std::stringstream log_msg;
        log_msg << "ping failed, redis response:[" << replyInfo.resultString << "].";
        LOG_WRITE_ERROR(log_msg.str());
        return DoRedisCmdResultType::Fail;
    }
	return DoRedisCmdResultType::Success;
}

bool RedisClient::initMasterSlaves()
{
	assert(m_initMasterAddrGot);

	WriteGuard guard(m_rwMasterMutex);
	
	m_masterClusterId=m_initMasterAddr.serverIp+":"+toStr(m_initMasterAddr.serverPort);
	std::stringstream log_msg;
	log_msg<<"init master clusterId is "<<m_masterClusterId;
	LOG_WRITE_INFO(log_msg.str());

	vector<RedisServerInfo> slavesAddr;
	
	RedisProxyInfo& masterNode=m_dataNodes[m_masterClusterId];
	masterNode.connectIp = m_initMasterAddr.serverIp;
	masterNode.connectPort = m_initMasterAddr.serverPort;
	masterNode.proxyId = m_masterClusterId;
	masterNode.isAlived = false;
	masterNode.clusterHandler = new RedisCluster;
		
	if (masterNode.clusterHandler == NULL)
	{
		return false;
	}
		
	if (!masterNode.clusterHandler->initConnectPool(masterNode.connectIp, masterNode.connectPort, m_connectionNum, m_connectTimeout, m_readTimeout, m_passwd))
	{
		LOG_WRITE_ERROR("init master node connection pool failed");
		return false;
	}

	masterNode.isAlived = true;

	if (m_passwd.empty() == false)
	{
		if(!AuthPasswd(m_passwd, masterNode.clusterHandler))
		{
			return false;
		}
	}

	if (!MasterGetReplicationSlavesInfo(masterNode.clusterHandler, slavesAddr))
	{
		LOG_WRITE_WARNING("get slave nodes failed");
		return true;
	}

	log_msg.str("");
	log_msg << "slaves count is " << slavesAddr.size();
	LOG_WRITE_INFO(log_msg.str());

	for (size_t i = 0; i < slavesAddr.size(); i++)
	{
		RedisProxyInfo slave_node;
		slave_node.connectIp = slavesAddr[i].serverIp;
		slave_node.connectPort = slavesAddr[i].serverPort;
		slave_node.proxyId = slave_node.connectIp + ":" + toStr(slave_node.connectPort);
		slave_node.isAlived = false;
		slave_node.clusterHandler=NULL;
		
//		slave_node.clusterHandler = new RedisCluster;
//		if (slave_node.clusterHandler == NULL)
//		{
//			LOG_WRITE_ERROR("new slave RedisCluster failed");
//		}
//		else if (!slave_node.clusterHandler->initConnectPool(slave_node.connectIp, slave_node.connectPort, m_connectionNum, m_connectTimeout, m_readTimeout))
//		{
//			LOG_WRITE_WARNING("init slave connection pool failed");
//		}
//		else
//		{
//			slave_node.isAlived = true;
//
//			if (m_passwd.empty() == false)
//			{
//				AuthPasswd(m_passwd, slave_node.clusterHandler);
//			}
//		}

		m_dataNodes[slave_node.proxyId] = slave_node;
	}

	return true;
}

bool RedisClient::freeRedisProxy()
{
	//	WriteGuard guard(m_rwProxyMutex);
	if (m_redisProxy.clusterHandler != NULL)
	{
		LOG_WRITE_INFO("freeRedisProxy now freeConnectionPool");
		m_redisProxy.clusterHandler->freeConnectPool();
		delete m_redisProxy.clusterHandler;
		m_redisProxy.clusterHandler = NULL;
	}

	return true;
}

bool RedisClient::freeRedisCluster()
{
	WriteGuard guard(m_rwClusterMutex);
	REDIS_CLUSTER_MAP::iterator clusterIter;
	for (clusterIter = m_clusterMap.begin(); clusterIter != m_clusterMap.end(); clusterIter++)
	{
		RedisClusterInfo clusterInfo = (*clusterIter).second;
		if (clusterInfo.clusterHandler != NULL)
		{
			clusterInfo.clusterHandler->freeConnectPool();
			delete clusterInfo.clusterHandler;
			clusterInfo.clusterHandler = NULL;
		}
	}
	m_clusterMap.clear();
	if (m_checkClusterNodesThreadStarted)
	{
		pthread_cancel(m_checkClusterNodesThreadId);
		m_checkClusterNodesThreadStarted = false;
	}
	return true;
}

bool RedisClient::StopSentinelThreads()
{
	if (m_sentinelHealthCheckThreadStarted)
	{
		m_forceSentinelHealthCheckThreadExit = true;
		SignalToDoMasterCheck(); // wakeup thread
		::pthread_join(m_sentinelHealthCheckThreadId, NULL);
		
		m_sentinelHealthCheckThreadStarted=false;
		m_forceSentinelHealthCheckThreadExit=false;
	}

	if(m_subscribeSwitchMasterThreadRunning)
	{
		m_forceSubscribeThreadExit=true;
		::pthread_join(m_subscribeSwitchMasterThreadId, NULL);

		m_subscribeSwitchMasterThreadRunning=false;
		m_forceSubscribeThreadExit=false;
	}

	return true;
}

bool RedisClient::freeSentinels()
{
	WriteGuard guard(m_rwSentinelHandlers);
	for (map<string, RedisProxyInfo>::iterator it = m_sentinelHandlers.begin(); it != m_sentinelHandlers.end(); ++it)
	{
		if (it->second.clusterHandler)
		{
			it->second.clusterHandler->freeConnectPool();
			delete it->second.clusterHandler;
			it->second.clusterHandler = NULL;
		}
	}
	m_sentinelHandlers.clear();

	return true;
}

bool RedisClient::freeMasterSlaves()
{
	{
		WriteGuard guard(m_rwMasterMutex);
		m_masterClusterId.clear();
		for (map<string, RedisProxyInfo>::iterator it = m_dataNodes.begin(); it != m_dataNodes.end(); ++it)
		{
			if (it->second.clusterHandler)
			{
				if (it->second.isAlived)
					it->second.clusterHandler->freeConnectPool();
				delete it->second.clusterHandler;
				it->second.clusterHandler = NULL;
			}
		}
		m_dataNodes.clear();
	}
	m_initMasterAddr.serverIp.clear();
	m_initMasterAddrGot=false;
	return true;
}

bool RedisClient::SentinelGetMasterAddrByName(RedisCluster* cluster, RedisServerInfo& serverInfo)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	string cmd1 = "sentinel";
	fillCommandPara(cmd1.c_str(), cmd1.size(), paraList);
	paraLen += cmd1.size() + 10;
	string cmd2 = "get-master-addr-by-name";
	fillCommandPara(cmd2.c_str(), cmd2.size(), paraList);
	paraLen += cmd2.size() + 10;
	fillCommandPara(m_masterName.c_str(), m_masterName.length(), paraList);
	paraLen += m_masterName.length() + 10;

	RedisReplyInfo replyInfo;
	bool success = cluster->doRedisCommand(paraList, paraLen, replyInfo);
	freeCommandList(paraList);
	if (!success)
	{
//		LOG_WRITE_ERROR("sentinel get-master-addr-by-name failed");
		return false;
	}

	if (!ParseSentinelGetMasterReply(replyInfo, serverInfo))
	{
		freeReplyInfo(replyInfo);
		return false;
	}
	else
	{
		freeReplyInfo(replyInfo);
		return true;
	}
}

bool RedisClient::ParseSentinelGetMasterReply(const RedisReplyInfo& replyInfo, RedisServerInfo& serverInfo)
{
	std::stringstream log_msg;
	//log_msg << "sentinel get-master-addr-by-name command has replyType " << replyInfo.replyType << ", resultString " << replyInfo.resultString << ", intValue " << replyInfo.intValue;
	//LOG_WRITE_INFO(log_msg.str());

	if (replyInfo.replyType == REDIS_REPLY_ERROR)
	{
		LOG_WRITE_ERROR("get master : get empty list or set.");
		return false;
	}

	if (replyInfo.replyType != REDIS_REPLY_ARRAY)
	{
		log_msg.str("");
		log_msg << "get master : recv redis wrong reply type:[" << replyInfo.replyType << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	if (replyInfo.arrayList.size() != 2)
	{
		LOG_WRITE_ERROR("get master : wrong result array size");
		return false;
	}

	list<ReplyArrayInfo>::const_iterator arrayIter = replyInfo.arrayList.begin();
	serverInfo.serverIp = arrayIter->arrayValue;

	arrayIter++;
	serverInfo.serverPort = atoi(arrayIter->arrayValue);

//	log_msg.str("");
//	log_msg << "sentinel get-master-addr-by-name ok: " << serverInfo.serverIp << ", " << serverInfo.serverPort;
//	LOG_WRITE_INFO(log_msg.str());
	return true;
}


bool RedisClient::MasterGetReplicationSlavesInfo(RedisCluster* cluster, vector<RedisServerInfo>& slaves)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	string cmd = "info";
	fillCommandPara(cmd.c_str(), cmd.size(), paraList);
	paraLen += cmd.size() + 10;
	cmd = "replication";
	fillCommandPara(cmd.c_str(), cmd.size(), paraList);
	paraLen += cmd.size() + 10;

	RedisReplyInfo replyInfo;
	bool success = cluster->doRedisCommand(paraList, paraLen, replyInfo);
	freeCommandList(paraList);
	if (!success)
	{
		LOG_WRITE_ERROR("do info replication command failed");
		return false;
	}

	if (!ParseInfoReplicationReply(replyInfo, slaves))
	{
		freeReplyInfo(replyInfo);
		return false;
	}
	else
	{
		freeReplyInfo(replyInfo);
		return true;
	}
}

bool RedisClient::ParseInfoReplicationReply(const RedisReplyInfo& replyInfo, vector<RedisServerInfo>& slaves)
{
	std::stringstream log_msg;
	log_msg << "info replication has replyType " << replyInfo.replyType << ", resultString " << replyInfo.resultString << ", intValue " << replyInfo.intValue;
	LOG_WRITE_INFO(log_msg.str());

	if (replyInfo.replyType == REDIS_REPLY_ERROR)
	{
		LOG_WRITE_ERROR("get empty list or set.");
		return false;
	}

	if (replyInfo.replyType != REDIS_REPLY_STRING)
	{
		log_msg.str("");
		log_msg << "recv redis wrong reply type:[" << replyInfo.replyType << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	if (replyInfo.arrayList.empty())
	{
		LOG_WRITE_ERROR("recv empty arraylist");
		return false;
	}
	list<ReplyArrayInfo>::const_iterator iter = replyInfo.arrayList.begin();
	if ((*iter).replyType != REDIS_REPLY_STRING || (*iter).arrayLen == 0)
	{
		log_msg.str("");
		log_msg << "arraylist is wrong, replyType:[" << (*iter).replyType << "], arrayLen:[" << (*iter).arrayLen << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	//	const string& replicationMsg=replyInfo.resultString;
	const string& replicationMsg = iter->arrayValue;
	string::size_type startpos = 0;
	string::size_type endpos = replicationMsg.find("\n");
	size_t slavescount = 0;
	const string connected_slaves = "connected_slaves:";
	const string slave = "slave";
	const string role = "role:";
	while (endpos != string::npos)
	{
		string linemsg = replicationMsg.substr(startpos, endpos - startpos);
		if (linemsg == "\r")
			break;

		if (memcmp(linemsg.c_str(), connected_slaves.c_str(), connected_slaves.size()) == 0)
		{
			// [connected_slaves:2^M]
			string::size_type pos = linemsg.find('\r');
			string str = linemsg.substr(connected_slaves.size(), pos - connected_slaves.size());
			slavescount = atoi(str.c_str());
			std::stringstream log_msg;
			log_msg << "get connected_slaves count: " << slavescount;
			LOG_WRITE_INFO(log_msg.str());
		}
		else if (memcmp(linemsg.c_str(), slave.c_str(), slave.size()) == 0)
		{
			RedisServerInfo slaveaddr;
			// [slave0:ip=192.168.12.59,port=7101,state=online,offset=85978535,lag=0^M]
			do {
				string::size_type pos1 = linemsg.find("ip=", 0);
				if (pos1 == string::npos)
					break;
				string::size_type pos2 = linemsg.find(',', pos1 + 3);
				if (pos2 == string::npos)
					break;
				slaveaddr.serverIp = linemsg.substr(pos1 + 3, pos2 - (pos1 + 3));

				pos1 = linemsg.find("port=", pos2);
				if (pos1 == string::npos)
					break;
				pos2 = linemsg.find(',', pos1 + 5);
				if (pos2 == string::npos)
					break;
				slaveaddr.serverPort = atoi(linemsg.substr(pos1 + 5, pos2 - (pos1 + 5)).c_str());

				std::stringstream log_msg;
				log_msg << "get one slave addr: " << slaveaddr.serverIp << ", " << slaveaddr.serverPort;
				LOG_WRITE_INFO(log_msg.str());
				slaves.push_back(slaveaddr);
			} while (0);
			if (slaves.size() >= slavescount)
				break;
		}
		else if (memcmp(linemsg.c_str(), role.c_str(), role.size()) == 0)
		{
			// role:master
			string::size_type pos = linemsg.find('\r');
			string str = linemsg.substr(role.size(), pos - role.size());
			if (str == "master")
			{
				LOG_WRITE_INFO("validate role is master ok");
			}
			else
			{
				std::stringstream log_msg;
				log_msg << "role is " << str;
				LOG_WRITE_ERROR(log_msg.str());
			}
		}

		startpos = endpos + 1;
		endpos = replicationMsg.find("\n", startpos);
	}

	return true;
}

bool RedisClient::AuthPasswd(const string& passwd, RedisCluster* cluster)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("auth", 4, paraList);
	paraLen += 15;
	fillCommandPara(passwd.c_str(), passwd.length(), paraList);
	paraLen += passwd.length() + 20;

	RedisReplyInfo replyInfo;
	bool success = cluster->doRedisCommand(paraList, paraLen, replyInfo);
	freeCommandList(paraList);
	if (!success)
		return false;
	success = ParseAuthReply(replyInfo);
	freeReplyInfo(replyInfo);
	if (success)
	{
		std::stringstream log_msg;
		log_msg << "auth passwd " << passwd << " success";
		LOG_WRITE_INFO(log_msg.str());
	}
	return success;
}

bool RedisClient::ParseAuthReply(const RedisReplyInfo& replyInfo)
{
	if (replyInfo.replyType != REDIS_REPLY_STATUS)
	{
		std::stringstream log_msg;
		log_msg << "auth passwd failed, redis response:[" << replyInfo.resultString << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	return true;
}

bool RedisClient::CheckIfNoAuth(const RedisReplyInfo& replyInfo)
{
//	std::stringstream log_msg;
//    log_msg << "master node replyInfo has replyType " << replyInfo.replyType << ", resultString " << replyInfo.resultString << ", intValue " << replyInfo.intValue;
//    LOG_WRITE_INFO(log_msg.str());

	if(replyInfo.replyType == REDIS_REPLY_ERROR)
	{
		std::string noauthMsg="NOAUTH";
		int minLen=std::min(replyInfo.resultString.size(), noauthMsg.size());
		if(strncmp(replyInfo.resultString.c_str(), noauthMsg.c_str(), minLen)==0)
		{
			LOG_WRITE_WARNING("NOAUTH failure");
			return true;
		}
	}
		
	return false;
}

//template<typename DBSerialize>
//bool RedisClient::getSerialWithLock(const string & key,DBSerialize & serial,RedisLockInfo & lockInfo)
//{
//	//add for  redis Optimistic Lock command, includes watch key, get key, or unwatch,when get key failed.
//	bool success = false;
//	list<RedisCmdParaInfo> watchParaList;
//	int32_t watchParaLen = 0;
//	fillCommandPara("watch", 5, watchParaList);
//	watchParaLen += 18;
//	fillCommandPara(key.c_str(), key.length(), watchParaList);
//	watchParaLen += key.length() + 20;
//	success = doRedisCommandWithLock(key,watchParaLen,watchParaList,RedisCommandType::REDIS_COMMAND_WATCH,lockInfo);
//	freeCommandList(watchParaList);
//	if (!success)
//	{
//		std::stringstream log_msg;
//		log_msg<<"do watch key:"<<key<<" failed.";
//		LOG_WRITE_ERROR(log_msg.str());
//		return false;
//	}
//	list<RedisCmdParaInfo> getParaList;
//	int32_t getParaLen = 0;
//	fillCommandPara("get", 3, getParaList);
//	getParaLen += 15;
//	fillCommandPara(key.c_str(), key.length(), getParaList);
//	getParaLen += key.length() + 20;
//	success = doRedisCommandWithLock(key,getParaLen,getParaList,RedisCommandType::REDIS_COMMAND_GET,lockInfo,true,&serial);
//	freeCommandList(getParaList);
//	if (success)
//	{
//		std::stringstream log_msg;
//		log_msg<<"get key:"<<key<<" serial with optimistic lock success.";
//		LOG_WRITE_INFO(log_msg.str());
//	}
//	else
//	{
//		std::stringstream log_msg;
//		log_msg<<"get key:"<<key<<" serial with optimistic lock failed.";
//		LOG_WRITE_ERROR(log_msg.str());
//		
//		list<RedisCmdParaInfo> unwatchParaList;
//		int32_t unwatchParaLen = 0;
//		fillCommandPara("unwatch", 7, unwatchParaList);
//		unwatchParaLen += 20;
//		doRedisCommandWithLock(key,unwatchParaLen,unwatchParaList,RedisCommandType::REDIS_COMMAND_UNWATCH,lockInfo);
//		freeCommandList(unwatchParaList);
//	}
//	return success;
//}
//
//template<typename DBSerialize>
//bool RedisClient::setSerialWithLock(const string & key, const DBSerialize& serial,RedisLockInfo & lockInfo)
//{
//	//add for  redis Optimistic Lock command, includes multi, set key, exec.
//	bool success = false;
//	//do multi command.
//	list<RedisCmdParaInfo> multiParaList;
//	int32_t multiParaLen = 0;
//	fillCommandPara("multi", 5, multiParaList);
//	multiParaLen += 18;
//	success = doRedisCommandWithLock(key,multiParaLen,multiParaList,RedisCommandType::REDIS_COMMAND_MULTI,lockInfo);
//	freeCommandList(multiParaList);
//	if (!success)
//	{
//		std::stringstream log_msg;
//		log_msg<<"do multi key:"<<key<<" failed.";
//		LOG_WRITE_ERROR(log_msg.str());
//		
//		list<RedisCmdParaInfo> unwatchParaList;
//		int32_t unwatchParaLen = 0;
//		fillCommandPara("unwatch", 7, unwatchParaList);
//		unwatchParaLen += 20;
//		doRedisCommandWithLock(key,unwatchParaLen,unwatchParaList,RedisCommandType::REDIS_COMMAND_UNWATCH,lockInfo);
//		freeCommandList(unwatchParaList);
//		return false;
//	}
//	//do set key value
//	list<RedisCmdParaInfo> setParaList;
//	int32_t setParaLen = 0;
//	fillCommandPara("set", 3, setParaList);
//	setParaLen += 15;
//	fillCommandPara(key.c_str(), key.length(), setParaList);
//	setParaLen += key.length() + 20;
//	DBOutStream out;
//	//serial.save(out);
//	save(serial, out);
//	fillCommandPara(out.getData(), out.getSize(), setParaList);
//	setParaLen += out.getSize() + 20;
//	success = doRedisCommandWithLock(key,setParaLen,setParaList,RedisCommandType::REDIS_COMMAND_SET,lockInfo);
//	freeCommandList(setParaList);
//	if (success)
//	{
//		std::stringstream log_msg;
//		log_msg<<"set key:"<<key<<" serial with optimistic lock success.";
//		LOG_WRITE_INFO(log_msg.str());
//	}
//	else
//	{
//		std::stringstream log_msg;
//		log_msg<<"set key:"<<key<<" serial with optimistic lock failed.";
//		LOG_WRITE_ERROR(log_msg.str());
//		
//		list<RedisCmdParaInfo> unwatchParaList;
//		int32_t unwatchParaLen = 0;
//		fillCommandPara("unwatch", 7, unwatchParaList);
//		unwatchParaLen += 20;
//		doRedisCommandWithLock(key,unwatchParaLen,unwatchParaList,RedisCommandType::REDIS_COMMAND_UNWATCH,lockInfo);
//		freeCommandList(unwatchParaList);
//		return false;
//	}
//	//do exec,need check exec response.
//	list<RedisCmdParaInfo> execParaList;
//	int32_t execParaLen = 0;
//	fillCommandPara("exec", 4, execParaList);
//	execParaLen += 18;
//	success = doRedisCommandWithLock(key,execParaLen,execParaList,RedisCommandType::REDIS_COMMAND_EXEC,lockInfo);
//	freeCommandList(execParaList);
//	if (!success)
//	{
//		std::stringstream log_msg;
//		log_msg<<"update data error,key["<<key<<"]";
//		LOG_WRITE_ERROR(log_msg.str());
//        return false;
//	}
//	return success;
//}
//
//bool RedisClient::releaseLock(const string & key,RedisLockInfo & lockInfo)
//{
//	if (lockInfo.clusterId.empty())
//	{
//		std::stringstream log_msg;
//		log_msg<<"lock cluster id is empty.";
//		LOG_WRITE_ERROR(log_msg.str());
//		return false;
//	}
//	list<RedisCmdParaInfo> unwatchParaList;
//	int32_t unwatchParaLen = 0;
//	fillCommandPara("unwatch", 7, unwatchParaList);
//	unwatchParaLen += 20;
//	doRedisCommandWithLock(key,unwatchParaLen,unwatchParaList,RedisCommandType::REDIS_COMMAND_UNWATCH,lockInfo);
//	freeCommandList(unwatchParaList);
//	return true;
//}

DoRedisCmdResultType RedisClient::find(const string& key)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("exists", 6, paraList);
	paraLen += 15;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;
	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_EXISTS);
	freeCommandList(paraList);
	return success;
}

bool RedisClient::delKeys(const string& keyPrefix)
{
	list<string> keys;
	getKeys(keyPrefix, keys);

	list<string>::iterator it;
	for (it = keys.begin(); it != keys.end(); it++)
	{
		del(*it);
	}
	keys.clear();
	return true;
}


DoRedisCmdResultType RedisClient::setSerial(const string& key, const string& value)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("set", 3, paraList);
	paraLen += 15;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;
	fillCommandPara(value.c_str(), value.size(), paraList);
	paraLen += value.size() + 20;
	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_SET);
	freeCommandList(paraList);
	return success;
}

DoRedisCmdResultType RedisClient::del(const string& key)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("del", 3, paraList);
	paraLen += 15;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;
	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_DEL);
	freeCommandList(paraList);
	return success;
}

DoRedisCmdResultType RedisClient::doRedisCommand(const string& key,
	int32_t commandLen,
	list < RedisCmdParaInfo >& commandList,
	RedisCommandType commandType)
{
	list<string> members;
	SerializableType* p = NULL;
	return doRedisCommand(key, commandLen, commandList, commandType, members, p, NULL);
}


DoRedisCmdResultType RedisClient::doRedisCommand(const string& key,
	int32_t commandLen,
	list < RedisCmdParaInfo >& commandList,
	RedisCommandType commandType,
	int* count)
{
	list<string> members;
	SerializableType* p = NULL;
	return doRedisCommand(key, commandLen, commandList, commandType, members, p, count);
}

DoRedisCmdResultType RedisClient::doRedisCommand(const string& key,
	int32_t commandLen,
	list < RedisCmdParaInfo >& commandList,
	RedisCommandType commandType,
	list<string>& members)
{
	SerializableType* p = NULL;
	return doRedisCommand(key, commandLen, commandList, commandType, members, p, NULL);
}

bool RedisClient::CreateConnectionPool(RedisProxyInfo& node, const std::string& passwd)
{
	if (node.clusterHandler == NULL)
	{
		node.clusterHandler = new RedisCluster();
		if (node.clusterHandler == NULL)
			return false;
	}

	if (!node.clusterHandler->initConnectPool(node.connectIp, node.connectPort, m_connectionNum, m_connectTimeout, m_readTimeout, passwd))
	{
		std::stringstream log_msg;
		log_msg << "init connection pool failed for " << node.proxyId;
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	node.isAlived = true;
	return true;
}

//bool RedisClient::StartCheckMasterThread()
//{
//	if (m_checkMasterNodeThreadStarted)
//		return true;
//	m_forceCheckMasterNodeThreadExit=false;
//	int ret = pthread_create(&m_checkMasterNodeThreadId, NULL, CheckMasterNodeThreadFunc, this);
//	if (ret)
//	{
//		return false;
//	}
//	m_checkMasterNodeThreadStarted = true;
//	return true;
//}

bool RedisClient::StartCheckClusterThread()
{
	if (m_checkClusterNodesThreadStarted)
		return true;
	int ret = pthread_create(&m_checkClusterNodesThreadId, NULL, CheckClusterNodesThreadWork, this);
	if (ret)
	{
		std::stringstream log_msg;
		log_msg << "start check cluster nodes thread failed: " << ret;
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	pthread_detach(m_checkClusterNodesThreadId);
	m_checkClusterNodesThreadStarted = true;
	return true;
}

void RedisClient::SignalToDoClusterNodes()
{
	MutexLockGuard guard(&m_lockCheckClusterSignalQueue);
	if (m_checkClusterSignalQueue.empty())
	{
		m_checkClusterSignalQueue.push(1);
	}
	m_condCheckClusterSignalQueue.SignalAll();
}

void RedisClient::SignalToDoMasterCheck()
{
	MutexLockGuard guard(&m_lockCheckMasterSignalQueue);
	if (m_checkMasterSignalQueue.empty())
	{
		m_checkMasterSignalQueue.push(1);
	}
	m_condCheckMasterSignalQueue.SignalAll();
}


DoRedisCmdResultType RedisClient::DoConnectMasterNode()
{
	ReadGuard guard(m_rwSentinelHandlers);
	// here we only check NoAuth, which is unrecoverable now
	DoRedisCmdResultType ret = DoRedisCmdResultType::Fail;
	for (map<string, RedisProxyInfo>::iterator it = m_sentinelHandlers.begin(); it != m_sentinelHandlers.end(); ++it)
	{
		if (!it->second.isAlived)
			continue;
		
		RedisServerInfo masterInfo;
		if (SentinelGetMasterAddrByName(it->second.clusterHandler, masterInfo)  &&  CheckMasterRole(masterInfo))
		{
			DoRedisCmdResultType result = DoSwitchMaster(masterInfo);
			if(result == DoRedisCmdResultType::Success)
			{
				return DoRedisCmdResultType::Success;
			}
			else if( result == DoRedisCmdResultType::NoAuth)
			{
				ret = DoRedisCmdResultType::NoAuth;
			}
		}
		
	}
	LOG_WRITE_ERROR("connect master nodes by sentinel reply all failed");
	return ret;
}

void* RedisClient::CheckClusterNodesThreadWork(void* arg)
{
	LOG_WRITE_INFO("check cluster nodes thread start");

	RedisClient* client = (RedisClient*)arg;

	while (true)
	{
		{
			MutexLockGuard guard(&(client->m_lockCheckClusterSignalQueue));
			while (client->m_checkClusterSignalQueue.empty())
			{
				client->m_condCheckClusterSignalQueue.Wait();
			}
		}

		std::queue<int> empty_queue;
		client->m_checkClusterSignalQueue.swap(empty_queue);

	//	client->m_checkClusterSignalQueue = std::move(std::queue<int>());

		LOG_WRITE_INFO("now recv signal to do cluster nodes command");

		client->DoCheckClusterNodes();
	}
	return 0;
}

void RedisClient::DoCheckClusterNodes()
{
	releaseUnusedClusterHandler();

	REDIS_CLUSTER_MAP clusterMap;
	if (!getRedisClustersByCommand(clusterMap))
	{
		LOG_WRITE_ERROR("get redis clusters by command failed.");
		return;
	}
	REDIS_CLUSTER_MAP oldClusterMap;
	getRedisClusters(oldClusterMap);
	//check cluster if change.
	bool change = false;
	REDIS_CLUSTER_MAP::iterator clusterIter;
	for (clusterIter = clusterMap.begin(); clusterIter != clusterMap.end(); clusterIter++)
	{
		string clusterId = (*clusterIter).first;
		//check clusterId if exist.
		if (oldClusterMap.find(clusterId) == oldClusterMap.end())
		{
			change = true;
			break;
		}
		else
		{
			//check slot or master,alive status if change.
			RedisClusterInfo clusterInfo = (*clusterIter).second;
			RedisClusterInfo oldClusterInfo = oldClusterMap[clusterId];
			if (clusterInfo.clusterId != oldClusterInfo.clusterId
				|| clusterInfo.isMaster != oldClusterInfo.isMaster
				|| clusterInfo.isAlived != oldClusterInfo.isAlived
				|| clusterInfo.masterClusterId != oldClusterInfo.masterClusterId
				|| clusterInfo.scanCursor != oldClusterInfo.scanCursor)
			{
				change = true;
				break;
			}
			//check bak cluster list if change.first check new cluster info
			list<string>::iterator bakIter;
			for (bakIter = clusterInfo.bakClusterList.begin(); bakIter != clusterInfo.bakClusterList.end(); bakIter++)
			{
				bool find = false;
				list<string>::iterator oldBakIter;
				for (oldBakIter = oldClusterInfo.bakClusterList.begin(); oldBakIter != oldClusterInfo.bakClusterList.end(); oldBakIter++)
				{
					if ((*bakIter) == (*oldBakIter))
					{
						find = true;
						break;
					}
				}
				if (!find)
				{
					change = true;
					break;
				}
			}
			if (change)
			{
				break;
			}
			//second check old cluster info.
			for (bakIter = oldClusterInfo.bakClusterList.begin(); bakIter != oldClusterInfo.bakClusterList.end(); bakIter++)
			{
				bool find = false;
				list<string>::iterator iter;
				for (iter = clusterInfo.bakClusterList.begin(); iter != clusterInfo.bakClusterList.end(); iter++)
				{
					if ((*bakIter) == (*iter))
					{
						find = true;
						break;
					}
				}
				if (!find)
				{
					change = true;
					break;
				}
			}
			if (change)
			{
				break;
			}
			//check slot map,first check new cluster slot map
			map<uint16_t, uint16_t>::iterator slotIter;
			for (slotIter = clusterInfo.slotMap.begin(); slotIter != clusterInfo.slotMap.end(); slotIter++)
			{
				uint16_t startSlotNum = (*slotIter).first;
				if (oldClusterInfo.slotMap.find(startSlotNum) == oldClusterInfo.slotMap.end())
				{
					change = true;
					break;
				}
				else
				{
					if ((*slotIter).second != oldClusterInfo.slotMap[startSlotNum])
					{
						change = true;
						break;
					}
				}
			}
			if (change)
			{
				break;
			}
			//second check old cluster slot map.
			for (slotIter = oldClusterInfo.slotMap.begin(); slotIter != oldClusterInfo.slotMap.end(); slotIter++)
			{
				uint16_t startSlotNum = (*slotIter).first;
				if (clusterInfo.slotMap.find(startSlotNum) == clusterInfo.slotMap.end())
				{
					change = true;
					break;
				}
				else
				{
					if ((*slotIter).second != clusterInfo.slotMap[startSlotNum])
					{
						change = true;
						break;
					}
				}
			}
			if (change)
			{
				break;
			}
		}
	}

	if (change)
	{
		checkAndSaveRedisClusters(clusterMap);
		return;
	}
	for (clusterIter = oldClusterMap.begin(); clusterIter != oldClusterMap.end(); clusterIter++)
	{
		string clusterId = (*clusterIter).first;
		//check clusterId if exist.
		if (clusterMap.find(clusterId) == clusterMap.end())
		{
			change = true;
			break;
		}
	}
	if (change)
	{
		checkAndSaveRedisClusters(clusterMap);
		return;
	}
}

bool RedisClient::getClusterIdFromRedirectReply(const string& redirectInfo, string& clusterId)
{
	string::size_type pos = redirectInfo.find(":");
	if (pos == string::npos)
		return false;
	string redirectIp = redirectInfo.substr(0, pos);
	string redirectPort = redirectInfo.substr(pos + 1);
	for (REDIS_CLUSTER_MAP::iterator it = m_clusterMap.begin(); it != m_clusterMap.end(); it++)
	{
		if (it->second.connectIp == redirectIp && (uint16_t)(atoi(redirectPort.c_str())) == (uint16_t)it->second.connectPort)
		{
			clusterId = it->second.clusterId;
			return true;
		}
	}
	return false;
}

//template<typename DBSerialize>
//bool RedisClient::doRedisCommandWithLock(const string & key,int32_t commandLen,list < RedisCmdParaInfo > & commandList,int commandType,RedisLockInfo & lockInfo,bool getSerial,DBSerialize * serial)
//{
//	assert(m_redisMode==CLUSTER_MODE);
//	//first calc crc16 value.
//	uint16_t crcValue = crc16(key.c_str(), key.length());
//	crcValue %= REDIS_SLOT_NUM;
//	string clusterId;
//	if (lockInfo.clusterId.empty())
//	{
//		if (!getClusterIdBySlot(crcValue, clusterId))
//		{
//			std::stringstream log_msg;
//			log_msg<<"key:["<<key<<"] hit slot:["<<crcValue<<"] select cluster failed.";
//			LOG_WRITE_ERROR(log_msg.str());
//			return false;
//		}
//	}
//	else
//	{
//		clusterId = lockInfo.clusterId;
//	}
//	//add for redirect end endless loop;
//	bool release = false;
//	if (commandType == RedisCommandType::REDIS_COMMAND_EXEC || commandType == RedisCommandType::REDIS_COMMAND_UNWATCH)
//	{
//		release = true;
//	}
//	vector<string> redirects;
//REDIS_COMMAND:
//	RedisReplyInfo replyInfo;
//
//	std::stringstream log_msg;
//	log_msg<<"key:["<<key<<"] hit slot:["<<crcValue<<"] select cluster.";
//	LOG_WRITE_INFO(log_msg.str());
//	
//	list<string> bakClusterList;
//	bakClusterList.clear();
//	list<string>::iterator bakIter;
//
//	//get cluster
//	RedisClusterInfo clusterInfo;
//	if (getRedisClusterInfo(clusterId,clusterInfo))
//	{
//		if(!clusterInfo.clusterHandler->doRedisCommandOneConnection(commandList, commandLen, replyInfo, release, &lockInfo.connection))
//		{
//			freeReplyInfo(replyInfo);
//			
//			//need check 
//			if (!lockInfo.clusterId.empty())
//			{
//				std::stringstream log_msg;
//				log_msg<<"the transaction must do in one connection.";
//				LOG_WRITE_ERROR(log_msg.str());
//				return false;
//			}
//			//need send to another cluster. check bak cluster.
//			if (clusterInfo.bakClusterList.size() != 0)
//			{
//				bakClusterList = clusterInfo.bakClusterList;
//				bakIter = bakClusterList.begin();
//				if (bakIter != bakClusterList.end())
//				{
//					clusterId = (*bakIter);
//					goto REDIS_COMMAND;
//				}
//				else
//				{
//					return false;
//				}
//			}
//			else
//			{
//				if (bakClusterList.size() > 0)
//				{
//					bakIter++;
//					if (bakIter != bakClusterList.end())
//					{
//						clusterId = (*bakIter);
//						goto REDIS_COMMAND;
//					}
//					else
//					{
//						return false;
//					}
//				}
//			}
//		}
//		bool needRedirect = false;
//		string redirectInfo;
//		redirectInfo.clear();
//		//switch command type
//		switch (commandType)
//		{
//			case RedisCommandType::REDIS_COMMAND_GET:
//				if(!parseGetSerialReply(replyInfo,*serial,needRedirect,redirectInfo))
//				{
//					freeReplyInfo(replyInfo);
//					return false;
//				}
//				break;
//			case RedisCommandType::REDIS_COMMAND_SET:
//				if(!parseSetSerialReply(replyInfo,needRedirect,redirectInfo))
//				{
//					freeReplyInfo(replyInfo);
//					return false;
//				}
//				break;
//			case RedisCommandType::REDIS_COMMAND_WATCH:
//				if(!parseStatusResponseReply(replyInfo,needRedirect,redirectInfo))
//				{
//					freeReplyInfo(replyInfo);
//					return false;
//				}
//				break;
//			case RedisCommandType::REDIS_COMMAND_UNWATCH:
//				if(!parseStatusResponseReply(replyInfo,needRedirect,redirectInfo))
//				{
//					freeReplyInfo(replyInfo);
//					return false;
//				}
//				//check UNWATCH reply,if not OK,need free connection.
//				if (replyInfo.resultString != "OK")
//				{
//					clusterInfo.clusterHandler->freeConnection(lockInfo.connection);
//				}
//				break;
//			case RedisCommandType::REDIS_COMMAND_MULTI:
//				if(!parseStatusResponseReply(replyInfo,needRedirect,redirectInfo))
//				{
//					freeReplyInfo(replyInfo);
//					return false;
//				}
//				break;
//			case RedisCommandType::REDIS_COMMAND_EXEC:
//				if(!parseExecReply(replyInfo,needRedirect,redirectInfo))
//				{
//					freeReplyInfo(replyInfo);
//					return false;
//				}
//				break;
//			default:
//				break;
//		}
//			
//		freeReplyInfo(replyInfo);
//		if (needRedirect)
//		{
//			
//			if (!lockInfo.clusterId.empty())
//			{
//				return false;
//			}
//			clusterId = redirectInfo;
//			//check cluster redirect if exist.
//			vector<string>::iterator reIter;
//			reIter = ::find(redirects.begin(), redirects.end(), redirectInfo);
//			if (reIter == redirects.end())
//			{
//				redirects.push_back(redirectInfo);
//				goto REDIS_COMMAND;
//			}
//			else
//			{
//			}
//		}
//	}
//	else
//	{
//		return false;
//	}
//	if (lockInfo.clusterId.empty())
//		lockInfo.clusterId = clusterId;
//	return true;
//}


// redis-server> scan m_cursor MATCH m_redisDbPre+index* COUNT count
void RedisClient::fillScanCommandPara(int cursor, const string& queryKey,
	int count, list<RedisCmdParaInfo>& paraList, int32_t& paraLen,
	ScanMode scanMode)
{
	int int32_t_max_size = 10; // 4 294 967 296, 10 chars
	paraList.clear();
	paraLen = 0;
	string str = "scan";
	fillCommandPara(str.c_str(), str.size(), paraList);
	paraLen += str.size() + int32_t_max_size + 1;

	char buf[20];
	if (scanMode == SCAN_NOLOOP)
		cursor = 0;
	int len = snprintf(buf, sizeof(buf) / sizeof(char), "%d", cursor);
	fillCommandPara(buf, len, paraList);
	paraLen += len + int32_t_max_size + 1;

	str = "MATCH";
	fillCommandPara(str.c_str(), str.size(), paraList);;
	paraLen += str.size() + int32_t_max_size + 1;

	string key = queryKey + "*";
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + int32_t_max_size + 1;

	str = "COUNT";
	fillCommandPara(str.c_str(), str.size(), paraList);
	paraLen += str.size() + int32_t_max_size + 1;

	len = snprintf(buf, sizeof(buf) / sizeof(char), "%d", count);
	fillCommandPara(buf, len, paraList);
	paraLen += len + int32_t_max_size + 1;
}


bool RedisClient::scanKeys(const string& queryKey, uint32_t count, list<string>& keys, ScanMode scanMode)
{
	std::stringstream log_msg;
	uint32_t COUNT = 10000; // TODO change according to db size or a maximum number
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	REDIS_CLUSTER_MAP clusterMap;
	getRedisClusters(clusterMap);
	REDIS_CLUSTER_MAP::iterator clusterIter;
	int retCursor = -1;

	log_msg << "clusterMap size is " << clusterMap.size();
	LOG_WRITE_INFO(log_msg.str());

	for (clusterIter = clusterMap.begin(); clusterIter != clusterMap.end(); /*clusterIter++*/)
	{
		string clusterId = (*clusterIter).first;
		RedisClusterInfo clusterInfo;
		if (!getRedisClusterInfo(clusterId, clusterInfo))
		{
			log_msg.str("");
			log_msg << "get redis cluster:[" << clusterId << "] info failed.";
			LOG_WRITE_WARNING(log_msg.str());
			continue;
		}

		if (clusterInfo.isMaster && clusterInfo.isAlived)
		{
			log_msg.str("");
			log_msg << "redis " << clusterId << " is master, alive";
			LOG_WRITE_INFO(log_msg.str());

			RedisReplyInfo replyInfo;
			fillScanCommandPara(clusterInfo.scanCursor, queryKey, COUNT, paraList, paraLen, scanMode);
			if (!clusterInfo.clusterHandler->doRedisCommand(paraList, paraLen, replyInfo, RedisConnection::SCAN_PARSER))
			{
				freeReplyInfo(replyInfo);

				log_msg.str("");
				log_msg << clusterId << " doRedisCommand failed, send bak redis cluster";
				LOG_WRITE_WARNING(log_msg.str());

				list<string>::iterator bakIter;
				for (bakIter = clusterInfo.bakClusterList.begin(); bakIter != clusterInfo.bakClusterList.end(); /*bakIter++*/)
				{
					RedisClusterInfo bakClusterInfo;
					if (!getRedisClusterInfo((*bakIter), bakClusterInfo))
					{
						log_msg.str("");
						log_msg << "get bak redis cluster:[" << *bakIter << "] info.";
						LOG_WRITE_WARNING(log_msg.str());
						bakIter++;
						continue;
					}
					fillScanCommandPara(bakClusterInfo.scanCursor, queryKey, COUNT, paraList, paraLen, scanMode);
					if (!bakClusterInfo.clusterHandler->doRedisCommand(paraList, paraLen, replyInfo, RedisConnection::SCAN_PARSER))
					{
						freeReplyInfo(replyInfo);
						log_msg.str("");
						log_msg << "redis cluster " << clusterInfo.clusterId << ", bak redis cluster " << bakClusterInfo.clusterId << " doRedisCommand failed";
						LOG_WRITE_ERROR(log_msg.str());
						bakIter++;
						continue;
					}
					else
					{
						if (parseScanKeysReply(replyInfo, keys, retCursor))
							updateClusterCursor(clusterId, retCursor);
						freeReplyInfo(replyInfo);
						if (keys.size() >= count)
						{
							log_msg.str("");
							log_msg << "get enough " << count << " keys";
							LOG_WRITE_INFO(log_msg.str());
							break;
						}
						else if (retCursor != 0)
						{
							// continue scan this bak cluster
							continue;
						}
					}
				}
			}
			else
			{
				if (parseScanKeysReply(replyInfo, keys, retCursor))
					updateClusterCursor(clusterId, retCursor);
				freeReplyInfo(replyInfo);
				if (keys.size() >= count)
				{
					log_msg.str("");
					log_msg << "get enough " << count << " keys";
					LOG_WRITE_INFO(log_msg.str());
					break;
				}
				else if (retCursor != 0)
				{
					// continue scan this cluster
					continue;
				}
			}
		}
		else
		{
			log_msg.str("");
			log_msg << "redis " << clusterId << " is not alive master, isMaster " << clusterInfo.isMaster << ", isAlive " << clusterInfo.isAlived;
			LOG_WRITE_INFO(log_msg.str());
		}
		if (keys.size() >= count)
		{
			log_msg.str("");
			log_msg << "get enough " << count << " keys";
			LOG_WRITE_INFO(log_msg.str());
			break;
		}
		clusterIter++; // scan next cluster
	}
	freeCommandList(paraList);

	log_msg.str("");
	log_msg << "scan keys get " << keys.size() << " keys";
	LOG_WRITE_INFO(log_msg.str());
	return keys.size() > 0 ? true : false;
}

DoRedisCmdResultType RedisClient::getKeys(const string& queryKey, list < string >& keys)
{
	if (m_redisMode == CLUSTER_MODE)
		return getKeysInCluster(queryKey, keys);

	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("keys", 4, paraList);
	paraLen += 15;
	string key = queryKey + "*";
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;

	DoRedisCmdResultType success = doRedisCommand("", paraLen, paraList, RedisCommandType::REDIS_COMMAND_KEYS, keys);
	freeCommandList(paraList);
	return success;
}

DoRedisCmdResultType RedisClient::getKeysInCluster(const string& queryKey, list < string >& keys)
{
	//need send to all master cluster
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("keys", 4, paraList);
	paraLen += 15;
	string key = queryKey + "*";
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;
	//
	REDIS_CLUSTER_MAP clusterMap;
	getRedisClusters(clusterMap);
	REDIS_CLUSTER_MAP::iterator clusterIter;
	for (clusterIter = clusterMap.begin(); clusterIter != clusterMap.end(); clusterIter++)
	{
		string clusterId = (*clusterIter).first;
		RedisClusterInfo clusterInfo;
		if (!getRedisClusterInfo(clusterId, clusterInfo))
		{
			std::stringstream log_msg;
			log_msg << "get redis cluster:[" << clusterId << "] info.";
			LOG_WRITE_WARNING(log_msg.str());
			continue;
		}
		if (clusterInfo.isMaster && clusterInfo.isAlived)
		{
			RedisReplyInfo replyInfo;
			if (!clusterInfo.clusterHandler->doRedisCommand(paraList, paraLen, replyInfo))
			{
				freeReplyInfo(replyInfo);
				//need send to backup cluster.
				if (clusterInfo.bakClusterList.size() != 0)
				{
					bool sendBak = false;
					list<string>::iterator bakIter;
					for (bakIter = clusterInfo.bakClusterList.begin(); bakIter != clusterInfo.bakClusterList.end(); bakIter++)
					{
						RedisClusterInfo bakClusterInfo;
						if (!getRedisClusterInfo((*bakIter), bakClusterInfo))
						{
							std::stringstream log_msg;
							log_msg << "get bak redis cluster:[" << *bakIter << "] info.";
							LOG_WRITE_WARNING(log_msg.str());
							continue;
						}
						if (!bakClusterInfo.clusterHandler->doRedisCommand(paraList, paraLen, replyInfo))
						{
							freeReplyInfo(replyInfo);
							continue;
						}
						else
						{
							sendBak = true;
						}
					}
					if (!sendBak)
					{
						continue;
					}
				}
			}
			parseKeysCommandReply(replyInfo, keys);
			freeReplyInfo(replyInfo);
		}
	}
	freeCommandList(paraList);
	return DoRedisCmdResultType::Success;
}

void RedisClient::getRedisClusters(REDIS_CLUSTER_MAP& clusterMap)
{
	ReadGuard guard(m_rwClusterMutex);
	clusterMap = m_clusterMap;
	return;
}

bool RedisClient::getRedisClustersByCommand(REDIS_CLUSTER_MAP& clusterMap)
{
	assert(m_redisMode == CLUSTER_MODE);
	bool success = false;
	//send cluster nodes.
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("cluster", 7, paraList);
	fillCommandPara("nodes", 5, paraList);
	paraLen += 30;
	REDIS_CLUSTER_MAP oldClusterMap;
	getRedisClusters(oldClusterMap);
	REDIS_CLUSTER_MAP::iterator clusterIter;
	for (clusterIter = oldClusterMap.begin(); clusterIter != oldClusterMap.end(); clusterIter++)
	{
		RedisClusterInfo oldClusterInfo;
		string clusterId = (*clusterIter).first;
		if (!getRedisClusterInfo(clusterId, oldClusterInfo))
		{
			std::stringstream log_msg;
			log_msg << "get cluster:" << clusterId << " info failed.";
			LOG_WRITE_ERROR(log_msg.str());
			continue;
		}
		RedisReplyInfo replyInfo;
		if (!oldClusterInfo.clusterHandler->doRedisCommand(paraList, paraLen, replyInfo))
		{
			LOG_WRITE_WARNING("do get cluster nodes failed.");
			continue;
		}
		if (replyInfo.replyType == REDIS_REPLY_ERROR)
		{
			std::stringstream log_msg;
			log_msg << "recv redis error response:[" << replyInfo.resultString << "].";
			LOG_WRITE_ERROR(log_msg.str());
			freeReplyInfo(replyInfo);
			freeCommandList(paraList);
			return false;
		}
		if (replyInfo.replyType != REDIS_REPLY_STRING)
		{
			std::stringstream log_msg;
			log_msg << "recv redis wrong reply type:[" << replyInfo.replyType << "].";
			LOG_WRITE_ERROR(log_msg.str());
			freeReplyInfo(replyInfo);
			freeCommandList(paraList);
			return false;
		}
		//check if master,slave parameter may be not newest.
		list<ReplyArrayInfo>::iterator arrayIter = replyInfo.arrayList.begin();
		string str = (*arrayIter).arrayValue;
		if (str.find("myself,slave") != string::npos)
		{
			std::stringstream log_msg;
			log_msg << "the redis cluster:[" << (*clusterIter).first << "] is slave. not use it reply,look for master.";
			LOG_WRITE_WARNING(log_msg.str());
			freeReplyInfo(replyInfo);
			success = false;
			continue;
		}
		if (!parseClusterInfo(replyInfo, clusterMap))
		{
			LOG_WRITE_ERROR("parse cluster info from redis reply failed.");
			freeCommandList(paraList);
			return false;
		}
		freeReplyInfo(replyInfo);
		success = true;
		break;
	}
	freeCommandList(paraList);
	return success;
}

bool RedisClient::checkAndSaveRedisClusters(REDIS_CLUSTER_MAP& clusterMap)
{
	WriteGuard guard(m_rwClusterMutex);
	REDIS_CLUSTER_MAP::iterator clusterIter;
	{
		WriteGuard guard(m_rwSlotMutex);
		m_slotMap.clear();
		for (clusterIter = clusterMap.begin(); clusterIter != clusterMap.end(); clusterIter++)
		{
			string clusterId = (*clusterIter).first;
			RedisClusterInfo clusterInfo = (*clusterIter).second;
			if (m_clusterMap.find(clusterId) != m_clusterMap.end())
			{
				RedisClusterInfo oldClusterInfo = m_clusterMap[clusterId];
				clusterInfo.clusterHandler = oldClusterInfo.clusterHandler;
				clusterInfo.connectionNum = oldClusterInfo.connectionNum;
				clusterInfo.connectTimeout = oldClusterInfo.connectTimeout;
				clusterInfo.readTimeout = oldClusterInfo.readTimeout;
				m_clusterMap[clusterId] = clusterInfo;
				if (clusterInfo.isMaster)
				{
					map<uint16_t, uint16_t>::iterator iter;
					for (iter = clusterInfo.slotMap.begin(); iter != clusterInfo.slotMap.end(); iter++)
					{
						uint16_t startSlotNum = (*iter).first;
						uint16_t stopSlotNum = (*iter).second;
						for (int i = startSlotNum; i <= stopSlotNum; i++)
						{
							m_slotMap[i] = clusterId;
						}
					}
				}
			}
			else
			{
				//need create new connect pool.
				clusterInfo.clusterHandler = new RedisCluster();
				clusterInfo.connectionNum = m_connectionNum;
				clusterInfo.connectTimeout = m_connectTimeout;
				if (!clusterInfo.clusterHandler->initConnectPool(clusterInfo.connectIp, clusterInfo.connectPort, clusterInfo.connectionNum, clusterInfo.connectTimeout, clusterInfo.readTimeout))
				{
					std::stringstream log_msg;
					log_msg << "init cluster:[" << clusterId << "] connect pool failed.";
					LOG_WRITE_ERROR(log_msg.str());
					return false;
				}
				m_clusterMap[clusterId] = clusterInfo;
				if (clusterInfo.isMaster)
				{
					map<uint16_t, uint16_t>::iterator iter;
					for (iter = clusterInfo.slotMap.begin(); iter != clusterInfo.slotMap.end(); iter++)
					{
						uint16_t startSlotNum = (*iter).first;
						uint16_t stopSlotNum = (*iter).second;
						for (int i = startSlotNum; i <= stopSlotNum; i++)
						{
							m_slotMap[i] = clusterId;
						}
					}
				}
			}
		}
	}
	//check old cluster map if need free.
	list<string> unusedClusters;
	unusedClusters.clear();
	for (clusterIter = m_clusterMap.begin(); clusterIter != m_clusterMap.end(); clusterIter++)
	{
		string clusterId = (*clusterIter).first;
		RedisClusterInfo clusterInfo = (*clusterIter).second;
		if (clusterMap.find(clusterId) == clusterMap.end())
		{
			m_unusedHandlers[clusterId] = clusterInfo.clusterHandler;
			unusedClusters.push_back(clusterId);
		}
	}
	list<string>::iterator unusedIter;
	for (unusedIter = unusedClusters.begin(); unusedIter != unusedClusters.end(); unusedIter++)
	{
		m_clusterMap.erase((*unusedIter));
	}
	return true;
}

DoRedisCmdResultType RedisClient::setKeyExpireTime(const string& key, uint32_t expireTime)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("expire", 6, paraList);
	paraLen += 15;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;
	fillCommandPara(toStr(expireTime).c_str(), toStr(expireTime).length(), paraList);
	paraLen += toStr(expireTime).length() + 20;
	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_EXPIRE);
	freeCommandList(paraList);
	return success;
}

void RedisClient::releaseUnusedClusterHandler()
{
	WriteGuard guard(m_rwClusterMutex);
	list<string> needFreeClusters;
	needFreeClusters.clear();
	map<string, RedisCluster*>::iterator clusterIter;
	for (clusterIter = m_unusedHandlers.begin(); clusterIter != m_unusedHandlers.end(); clusterIter++)
	{
		if (((*clusterIter).second)->checkIfCanFree())
		{
			needFreeClusters.push_back((*clusterIter).first);
		}
	}
	list<string>::iterator iter;
	for (iter = needFreeClusters.begin(); iter != needFreeClusters.end(); iter++)
	{
		RedisCluster* clusterHandler = m_unusedHandlers[(*iter)];
		clusterHandler->freeConnectPool();
		delete clusterHandler;
		clusterHandler = NULL;
		m_unusedHandlers.erase((*iter));
	}
}

bool RedisClient::parseClusterInfo(RedisReplyInfo& replyInfo, REDIS_CLUSTER_MAP& clusterMap)
{
	//get reply string
	list<ReplyArrayInfo>::iterator iter = replyInfo.arrayList.begin();
	if (iter == replyInfo.arrayList.end())
	{
		LOG_WRITE_ERROR("reply not have array info.");
		return false;
	}
	if ((*iter).replyType != REDIS_REPLY_STRING || (*iter).arrayLen == 0)
	{
		std::stringstream log_msg;
		log_msg << "parse cluster info failed.redis reply info is wrong,replyType:[" << (*iter).replyType << "], arrayLen:[" << (*iter).arrayLen << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	std::stringstream log_msg;
	log_msg << "recv redis cluster response:[" << (*iter).arrayValue << "].";
	LOG_WRITE_INFO(log_msg.str());

	string str = (*iter).arrayValue;
	string::size_type startPos, findPos;
	startPos = findPos = 0;
	findPos = str.find("\n", startPos);
	map<string, string> bakMap; //for key is bak cluster address info, value is master cluster id.
	bakMap.clear();
	while (findPos != string::npos)
	{
		string infoStr = str.substr(startPos, findPos - startPos);
		if (infoStr == "\r")
			break;
		if (infoStr.find("fail") != string::npos
			|| infoStr.find("noaddr") != string::npos
			|| infoStr.find("disconnected") != string::npos)
		{
			startPos = findPos + 1;
			findPos = str.find("\n", startPos);
			continue;
		}
		RedisClusterInfo clusterInfo;
		if (!parseOneCluster(infoStr, clusterInfo))
		{
			LOG_WRITE_ERROR("parse one cluster failed.");
			clusterMap.clear();
			return false;
		}
		string clusterId = clusterInfo.connectIp + ":" + toStr(clusterInfo.connectPort);
		//check if bak cluster node info.
		if (!clusterInfo.isMaster && !clusterInfo.masterClusterId.empty())
		{
			bakMap[clusterId] = clusterInfo.masterClusterId;
		}
		clusterInfo.connectionNum = m_connectionNum;
		clusterInfo.connectTimeout = m_connectTimeout;
		clusterInfo.readTimeout = m_readTimeout;
		clusterMap[clusterId] = clusterInfo;
		startPos = findPos + 1;
		findPos = str.find("\n", startPos);
	}
	//
	map<string, string>::iterator bakIter;
	for (bakIter = bakMap.begin(); bakIter != bakMap.end(); bakIter++)
	{
		REDIS_CLUSTER_MAP::iterator clusterIter;
		for (clusterIter = clusterMap.begin(); clusterIter != clusterMap.end(); clusterIter++)
		{
			if (((*clusterIter).second).clusterId == (*bakIter).second)
			{
				((*clusterIter).second).bakClusterList.push_back((*bakIter).first);
			}
		}
	}
	return true;
}

bool RedisClient::parseOneCluster(const string& infoStr, RedisClusterInfo& clusterInfo)
{
	//first parse node id.
	string::size_type startPos, findPos;
	startPos = findPos = 0;
	findPos = infoStr.find(" ");
	if (findPos != string::npos)
	{
		clusterInfo.clusterId = infoStr.substr(0, findPos);
		startPos = findPos + 1;
		findPos = infoStr.find(" ", startPos);
		if (findPos == string::npos)
		{
			std::stringstream log_msg;
			log_msg << "parse one cluster:[" << infoStr << "] failed.";
			LOG_WRITE_ERROR(log_msg.str());
			return false;
		}
		//parse ip port
		string address = infoStr.substr(startPos, findPos - startPos);
		startPos = findPos + 1;
		findPos = address.find(":");
		if (findPos != string::npos)
		{
			clusterInfo.connectIp = address.substr(0, findPos);
			clusterInfo.connectPort = atoi(address.substr(findPos + 1, address.length() - findPos).c_str());
		}
		else
		{
			clusterInfo.connectIp = address;
			clusterInfo.connectPort = REDIS_DEFALUT_SERVER_PORT;
		}
		//parse master slave.
		findPos = infoStr.find(" ", startPos);
		if (findPos == string::npos)
		{
			return false;
		}
		string tmpStr;
		tmpStr = infoStr.substr(startPos, findPos - startPos);
		if (tmpStr.find("master") != string::npos)
		{
			clusterInfo.isMaster = true;
		}
		startPos = findPos + 1;
		findPos = infoStr.find(" ", startPos);
		if (findPos == string::npos)
		{
			return false;
		}
		if (!clusterInfo.isMaster)
		{
			clusterInfo.masterClusterId = infoStr.substr(startPos, findPos - startPos);
		}
		startPos = findPos + 1;
		//first find status.
		findPos = infoStr.find("disconnected", startPos);
		if (findPos != string::npos)
		{
			clusterInfo.isAlived = false;
		}
		else
		{
			clusterInfo.isAlived = true;
		}
		findPos = infoStr.find("connected", startPos);
		if (clusterInfo.isMaster && clusterInfo.isAlived)
		{
			startPos = findPos + 1;
			findPos = infoStr.find(" ", startPos);
			if (findPos == string::npos)
				return false;
			startPos = findPos + 1;
			findPos = infoStr.find(" ", startPos);
			string slotStr;
			uint16_t startSlotNum, stopSlotNum;
			while (findPos != string::npos)
			{
				slotStr = infoStr.substr(startPos, findPos);
				startSlotNum = stopSlotNum = -1;
				parseSlotStr(slotStr, startSlotNum, stopSlotNum);
				clusterInfo.slotMap[startSlotNum] = stopSlotNum;
				startPos = findPos + 1;

				std::stringstream log_msg;
				log_msg << "parse cluster slot success,cluster:[" << clusterInfo.clusterId << "] has slot[" << startSlotNum << "-" << stopSlotNum << "].";
				LOG_WRITE_INFO(log_msg.str());
				findPos = infoStr.find(" ", startPos);
			}
			slotStr = infoStr.substr(startPos, infoStr.length() - startPos);
			startSlotNum = stopSlotNum = -1;
			parseSlotStr(slotStr, startSlotNum, stopSlotNum);
			clusterInfo.slotMap[startSlotNum] = stopSlotNum;

			std::stringstream log_msg;
			log_msg << "parse cluster slot success,cluster:[" << clusterInfo.clusterId << "] has slot[" << startSlotNum << "-" << stopSlotNum << "].";
			LOG_WRITE_INFO(log_msg.str());
		}

		std::stringstream log_msg;
		log_msg << "parse cluster:[" << clusterInfo.clusterId << "] info success, cluster address:[" << clusterInfo.connectIp << ":" << clusterInfo.connectPort << "] master:[" << clusterInfo.isMaster << "], active:[" << clusterInfo.isAlived << "], master cluster id:[" << clusterInfo.masterClusterId << "].";
		LOG_WRITE_INFO(log_msg.str());
	}
	return true;
}

void RedisClient::parseSlotStr(string& slotStr, uint16_t& startSlotNum, uint16_t& stopSlotNum)
{
	string::size_type findPos;
	findPos = slotStr.find("-");
	if (findPos != string::npos)
	{
		startSlotNum = atoi(slotStr.substr(0, findPos).c_str());
		stopSlotNum = atoi(slotStr.substr(findPos + 1, slotStr.length() - findPos).c_str());
	}
}

// return true if get "MOVED" and redirectInfo, or false
bool RedisClient::checkIfNeedRedirect(RedisReplyInfo& replyInfo, bool& needRedirect, string& redirectInfo)
{
	//assert(m_redisMode==CLUSTER_MODE);
	if (m_redisMode == STAND_ALONE_OR_PROXY_MODE)
	{
		return false;
	}
	if (replyInfo.replyType == REDIS_REPLY_ERROR)
	{
		std::stringstream log_msg;
		log_msg << "recv redis error response:[" << replyInfo.resultString << "].";
		LOG_WRITE_WARNING(log_msg.str());
		//check if move response.
		if (strncasecmp(replyInfo.resultString.c_str(), "MOVED", 5) == 0)
		{
			needRedirect = true;
			string::size_type findPos;
			findPos = replyInfo.resultString.find_last_of(" ");
			if (findPos != string::npos)
			{
				redirectInfo = replyInfo.resultString.substr(findPos + 1, replyInfo.resultString.length() - findPos - 1);
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	return false;
}


DoRedisCmdResultType RedisClient::parseFindReply(RedisReplyInfo& replyInfo, bool& needRedirect, string& redirectInfo)
{
	if (checkIfNeedRedirect(replyInfo, needRedirect, redirectInfo))
	{
		std::stringstream log_msg;
		log_msg << "need direct to cluster:[" << redirectInfo << "].";
		LOG_WRITE_WARNING(log_msg.str());
		return DoRedisCmdResultType::Redirected;
	}
	if (replyInfo.replyType != REDIS_REPLY_INTEGER   && replyInfo.intValue == 0)
	{
		return DoRedisCmdResultType::NotFound;
	}
	
	return DoRedisCmdResultType::Success;
}

DoRedisCmdResultType RedisClient::parseSetSerialReply(RedisReplyInfo& replyInfo, bool& needRedirect, string& redirectInfo)
{
	if (checkIfNeedRedirect(replyInfo, needRedirect, redirectInfo))
	{
		std::stringstream log_msg;
		log_msg << "need direct to cluster:[" << redirectInfo << "].";
		LOG_WRITE_WARNING(log_msg.str());
		return DoRedisCmdResultType::Redirected;
	}
	if (replyInfo.replyType != REDIS_REPLY_STATUS)
	{
		std::stringstream log_msg;
		log_msg << "set serial failed, redis response:[" << replyInfo.resultString << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return DoRedisCmdResultType::Fail;
	}
	return DoRedisCmdResultType::Success;
}

bool RedisClient::parseScanKeysReply(RedisReplyInfo& replyInfo, list<string>& keys, int& retCursor)
{
	retCursor = -1;

	if (replyInfo.replyType != REDIS_REPLY_ARRAY)
	{
		std::stringstream log_msg;
		log_msg << "recv redis wrong reply type:[" << replyInfo.replyType << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	retCursor = replyInfo.intValue;
	list<ReplyArrayInfo>::iterator arrayIter;
	for (arrayIter = replyInfo.arrayList.begin(); arrayIter != replyInfo.arrayList.end(); arrayIter++)
	{
		std::stringstream log_msg;
		log_msg << "arrayList has replyType " << (*arrayIter).replyType << ", arrayValue " << arrayIter->arrayValue << ", arrayLen " << arrayIter->arrayLen;
		LOG_WRITE_INFO(log_msg.str());

		if ((*arrayIter).replyType == REDIS_REPLY_STRING)
		{
			string key = (*arrayIter).arrayValue;
			keys.push_back(key);
		}
	}
	return true;
}

bool RedisClient::parseKeysCommandReply(RedisReplyInfo& replyInfo, list < string >& keys)
{
//		std::stringstream log_msg;
//		log_msg<<"keys replyInfo has replyType "<<replyInfo.replyType<<", resultString "<<replyInfo.resultString<<", intValue "<<replyInfo.intValue;
//		LOG_WRITE_INFO(log_msg.str());

	if (replyInfo.replyType == REDIS_REPLY_ERROR)
	{
		LOG_WRITE_INFO("get empty list or set.");
		return true;
	}

	if (replyInfo.replyType != REDIS_REPLY_ARRAY)
	{
		std::stringstream log_msg;
		log_msg << "recv redis wrong reply type:[" << replyInfo.replyType << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	list<ReplyArrayInfo>::iterator arrayIter;
	for (arrayIter = replyInfo.arrayList.begin(); arrayIter != replyInfo.arrayList.end(); arrayIter++)
	{
		if ((*arrayIter).replyType == REDIS_REPLY_STRING)
		{
			string key = (*arrayIter).arrayValue;
//			LOG_WRITE_INFO(key);
			keys.push_back(key);
		}
	}
	return true;
}

//for watch,unwatch,multi command response.
bool RedisClient::parseStatusResponseReply(RedisReplyInfo& replyInfo, bool& needRedirect, string& redirectInfo)
{
	if (checkIfNeedRedirect(replyInfo, needRedirect, redirectInfo))
	{
		std::stringstream log_msg;
		log_msg << "need direct to cluster:[" << redirectInfo << "].";
		LOG_WRITE_WARNING(log_msg.str());
		return true;
	}
	if (replyInfo.replyType != REDIS_REPLY_STATUS)
	{
		std::stringstream log_msg;
		log_msg << "status response failed, redis response:[" << replyInfo.resultString << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	return true;
}

bool RedisClient::parseExecReply(RedisReplyInfo& replyInfo, bool& needRedirect, string& redirectInfo)
{
	if (checkIfNeedRedirect(replyInfo, needRedirect, redirectInfo))
	{
		std::stringstream log_msg;
		log_msg << "need direct to cluster:[" << redirectInfo << "].";
		LOG_WRITE_WARNING(log_msg.str());
		return true;
	}

	if (replyInfo.replyType != REDIS_REPLY_ARRAY)
	{
		std::stringstream log_msg;
		log_msg << "recv redis wrong reply type:[" << replyInfo.replyType << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	//parse exec reply
	if (replyInfo.replyType == REDIS_REPLY_ARRAY && replyInfo.intValue == -1)
	{
		LOG_WRITE_ERROR("exec reply -1,set serial exec failed.");
		return false;
	}
	//parse array list.
	list<ReplyArrayInfo>::iterator arrayIter;
	for (arrayIter = replyInfo.arrayList.begin(); arrayIter != replyInfo.arrayList.end(); arrayIter++)
	{
		if ((*arrayIter).replyType == REDIS_REPLY_STRING)
		{
			if (strncmp((*arrayIter).arrayValue, "-", 1) == 0 || strncmp((*arrayIter).arrayValue, ":0", 2) == 0)
			{
				std::stringstream log_msg;
				log_msg << "recv failed exec reply: " << (*arrayIter).arrayValue;
				LOG_WRITE_ERROR(log_msg.str());
				return false;
			}
		}
	}
	return true;
}

void RedisClient::freeReplyInfo(RedisReplyInfo& replyInfo)
{
	if (replyInfo.arrayList.size() > 0)
	{
		list<ReplyArrayInfo>::iterator iter;
		for (iter = replyInfo.arrayList.begin(); iter != replyInfo.arrayList.end(); iter++)
		{
			if ((*iter).arrayValue != NULL)
			{
				free((*iter).arrayValue);
				(*iter).arrayValue = NULL;
			}
		}
		replyInfo.arrayList.clear();
	}
}


void RedisClient::fillCommandPara(const char* paraValue, int32_t paraLen, list < RedisCmdParaInfo >& paraList, bool verboseLog)
{
	if(verboseLog)
	{
		std::stringstream log_msg;
		log_msg << "fillCommandPara : paraValue " << paraValue << ", paraLen " << paraLen;
		LOG_WRITE_INFO(log_msg.str());
	}

	RedisCmdParaInfo paraInfo;
	paraInfo.paraLen = paraLen;
	paraInfo.paraValue = (char*)malloc(paraLen + 1);
	memset(paraInfo.paraValue, 0, paraLen + 1);
	memcpy(paraInfo.paraValue, paraValue, paraLen);
	paraList.push_back(paraInfo);
}

void RedisClient::freeCommandList(list < RedisCmdParaInfo >& paraList)
{
	list<RedisCmdParaInfo>::iterator commandIter;
	for (commandIter = paraList.begin(); commandIter != paraList.end(); commandIter++)
	{
		free((*commandIter).paraValue);
		(*commandIter).paraValue = NULL;
	}
	paraList.clear();
}

bool RedisClient::getRedisClusterInfo(string& clusterId, RedisClusterInfo& clusterInfo)
{
	ReadGuard guard(m_rwClusterMutex);
	if (m_clusterMap.find(clusterId) != m_clusterMap.end())
	{
		clusterInfo = m_clusterMap[clusterId];
		return true;
	}

	std::stringstream log_msg;
	log_msg << "not find cluster:" << clusterId << " info.";
	LOG_WRITE_ERROR(log_msg.str());
	return false;
}

void RedisClient::updateClusterCursor(const string& clusterId, int newcursor)
{
	if (newcursor < 0)
		return;
	ReadGuard guard(m_rwClusterMutex);
	if (m_clusterMap.find(clusterId) != m_clusterMap.end())
	{
		m_clusterMap[clusterId].scanCursor = newcursor;
		return;
	}

	std::stringstream log_msg;
	log_msg << "updateClusterCursor non-exist redis cluster " << clusterId;
	LOG_WRITE_ERROR(log_msg.str());
}


bool RedisClient::getClusterIdBySlot(uint16_t slotNum, string& clusterId)
{
	assert(m_redisMode == CLUSTER_MODE);
	ReadGuard guard(m_rwSlotMutex);
	if (m_slotMap.find(slotNum) != m_slotMap.end())
	{
		clusterId = m_slotMap[slotNum];
		return true;
	}

	std::stringstream log_msg;
	log_msg << "slot:" << slotNum << " not hit any cluster.please check redis cluster.";
	LOG_WRITE_ERROR(log_msg.str());
	return false;
}

DoRedisCmdResultType RedisClient::zadd(const string& key, const string& member, int score)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("zadd", 4, paraList);
	paraLen += 16;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;

	char score_c[64] = { 0 };
	sprintf(score_c, "%d", score);
	fillCommandPara(score_c, strlen(score_c), paraList);
	paraLen += strlen(score_c) + 20;

	fillCommandPara(member.c_str(), member.length(), paraList);
	paraLen += member.length() + 20;

	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_ZADD);
	freeCommandList(paraList);
	return success;
}

DoRedisCmdResultType RedisClient::zrem(const string& key, const string& member)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("zrem", 4, paraList);
	paraLen += 16;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;
	fillCommandPara(member.c_str(), member.length(), paraList);
	paraLen += member.length() + 20;

	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_ZREM);
	freeCommandList(paraList);
	return success;
}

DoRedisCmdResultType RedisClient::zincby(const string& key, const string& member, int increment)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("zincrby", 7, paraList);
	paraLen += 19;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;

	char inc[64] = { 0 };
	sprintf(inc, "%d", increment);
	fillCommandPara(inc, strlen(inc), paraList);
	paraLen += strlen(inc) + 20;

	fillCommandPara(member.c_str(), member.length(), paraList);
	paraLen += member.length() + 20;

	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_ZINCRBY);
	freeCommandList(paraList);
	return success;
}

DoRedisCmdResultType RedisClient::zcount(const string& key, int start, int end, int* countp)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("zcount", 6, paraList);
	paraLen += 18;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;

	char start_c[64] = { 0 };
	sprintf(start_c, "%d", start);
	fillCommandPara(start_c, strlen(start_c), paraList);
	paraLen += strlen(start_c) + 20;
	char end_c[64] = { 0 };
	sprintf(end_c, "%d", end);
	fillCommandPara(end_c, strlen(end_c), paraList);
	paraLen += strlen(end_c) + 20;

	int count = 0;
	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_ZCOUNT, &count);
	freeCommandList(paraList);
	*countp=count;
	return success;
}

DoRedisCmdResultType RedisClient::zcard(const string& key, int* countp)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	string str = "zcard";
	fillCommandPara(str.c_str(), str.size(), paraList);
	paraLen += str.size() + 11;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;

	int count = 0;
	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_ZCARD, &count);
	freeCommandList(paraList);
	*countp=count;
	return success;
}

DoRedisCmdResultType RedisClient::dbsize(int* countp)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	string str = "DBSIZE";
	fillCommandPara(str.c_str(), str.size(), paraList);
	paraLen += str.size() + 11;

	int count = 0;
	DoRedisCmdResultType success = doRedisCommand("", paraLen, paraList, RedisCommandType::REDIS_COMMAND_DBSIZE, &count);
	freeCommandList(paraList);
	*countp=count;
	return success;
}

DoRedisCmdResultType RedisClient::zscore(const string& key, const string& member, int* countp)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("zscore", 6, paraList);
	paraLen += 18;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;
	fillCommandPara(member.c_str(), member.length(), paraList);
	paraLen += member.length() + 20;

	int count = -1;
	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_ZSCORE, &count);
	freeCommandList(paraList);
	*countp=count;
	return success;
}

DoRedisCmdResultType RedisClient::zrangebyscore(const string& key, int start, int end, list<string>& members)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("zrangebyscore", 13, paraList);
	paraLen += 25;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;

	char start_c[64] = { 0 };
	sprintf(start_c, "%d", start);
	fillCommandPara(start_c, strlen(start_c), paraList);
	paraLen += strlen(start_c) + 20;
	char end_c[64] = { 0 };
	sprintf(end_c, "%d", end);
	fillCommandPara(end_c, strlen(end_c), paraList);
	paraLen += strlen(end_c) + 20;

	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_ZRANGEBYSCORE, members);
	freeCommandList(paraList);
	return success;
}

DoRedisCmdResultType RedisClient::zremrangebyscore(const string& key, int start, int end)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("zremrangebyscore", 16, paraList);
	paraLen += 28;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;

	char start_c[64] = { 0 };
	sprintf(start_c, "%d", start);
	fillCommandPara(start_c, strlen(start_c), paraList);
	paraLen += strlen(start_c) + 20;
	char end_c[64] = { 0 };
	sprintf(end_c, "%d", end);
	fillCommandPara(end_c, strlen(end_c), paraList);
	paraLen += strlen(end_c) + 20;

	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_ZREMRANGEBYSCORE);
	freeCommandList(paraList);
	return success;
}


DoRedisCmdResultType RedisClient::sadd(const string& key, const string& member)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	string str = "sadd";
	fillCommandPara(str.c_str(), str.size(), paraList);
	paraLen += str.size() + 11;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;

	fillCommandPara(member.c_str(), member.length(), paraList);
	paraLen += member.length() + 20;

	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_SADD);
	freeCommandList(paraList);
	return success;
}

DoRedisCmdResultType RedisClient::srem(const string& key, const string& member)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	string str = "srem";
	fillCommandPara(str.c_str(), str.size(), paraList);
	paraLen += str.size() + 11;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;

	fillCommandPara(member.c_str(), member.length(), paraList);
	paraLen += member.length() + 20;

	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_SREM);
	freeCommandList(paraList);
	return success;
}

DoRedisCmdResultType RedisClient::scard(const string& key, int* countp)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	string str = "scard";
	fillCommandPara(str.c_str(), str.size(), paraList);
	paraLen += str.size() + 11;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;

	int count = 0;
	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_SCARD, &count);
	freeCommandList(paraList);
	*countp=count;
	return success;
}

DoRedisCmdResultType RedisClient::sismember(const string& key, const string& member)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	string str = "sismember";
	fillCommandPara(str.c_str(), str.size(), paraList);
	paraLen += str.size() + 11;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;

	fillCommandPara(member.c_str(), member.length(), paraList);
	paraLen += member.length() + 20;

	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_SISMEMBER);
	freeCommandList(paraList);
	return success;
}

DoRedisCmdResultType RedisClient::smembers(const string& key, list<string>& members)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	string str = "smembers";
	fillCommandPara(str.c_str(), str.size(), paraList);
	paraLen += str.size() + 11;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 11;

	DoRedisCmdResultType success = doRedisCommand(key, paraLen, paraList, RedisCommandType::REDIS_COMMAND_SMEMBERS, members);
	freeCommandList(paraList);
	return success;
}

/*
	API for transaction of STAND_ALONE_OR_PROXY_MODE
*/
bool RedisClient::PrepareTransaction(RedisConnection** conn)
{
	if (m_redisMode != STAND_ALONE_OR_PROXY_MODE)
	{
		LOG_WRITE_ERROR("transaction only supported in standalone mode");
		return false;
	}

	RedisConnection* con = m_redisProxy.clusterHandler->getUnreleasedConnection();
	if (con == NULL)
	{
		LOG_WRITE_ERROR("cannot acquire a redis connection");
		return false;
	}
	*conn = con;
	return true;
}

bool RedisClient::WatchKeys(const vector<string>& keys, RedisConnection* con)
{
	for (size_t i = 0; i < keys.size(); i++)
	{
		if (!WatchKey(keys[i], con))
			return false;
	}
	return true;
}

bool RedisClient::WatchKey(const string& key, RedisConnection* con)
{
	list<RedisCmdParaInfo> watchParaList;
	int32_t watchParaLen = 0;
	fillCommandPara("watch", 5, watchParaList);
	watchParaLen += 18;
	fillCommandPara(key.c_str(), key.length(), watchParaList);
	watchParaLen += key.length() + 20;
	bool success = doTransactionCommandInConnection(watchParaLen, watchParaList, RedisCommandType::REDIS_COMMAND_WATCH, con);
	freeCommandList(watchParaList);
	return success;
}

bool RedisClient::Unwatch(RedisConnection* con)
{
	list<RedisCmdParaInfo> unwatchParaList;
	int32_t unwatchParaLen = 0;
	fillCommandPara("unwatch", 6, unwatchParaList);
	unwatchParaLen += 20;
	bool success = doTransactionCommandInConnection(unwatchParaLen, unwatchParaList, RedisCommandType::REDIS_COMMAND_UNWATCH, con);
	freeCommandList(unwatchParaList);
	return success;
}


bool RedisClient::StartTransaction(RedisConnection* con)
{
	if (m_redisMode != STAND_ALONE_OR_PROXY_MODE)
	{
		return false;
	}

	bool success = false;
	list<RedisCmdParaInfo> multiParaList;
	int32_t multiParaLen = 0;
	fillCommandPara("multi", 5, multiParaList);
	multiParaLen += 18;

	success = doTransactionCommandInConnection(multiParaLen, multiParaList, RedisCommandType::REDIS_COMMAND_MULTI, con);
	freeCommandList(multiParaList);
	return success;
}


bool RedisClient::DiscardTransaction(RedisConnection* con)
{
	if (m_redisMode != STAND_ALONE_OR_PROXY_MODE)
	{
		return false;
	}

	bool success = false;
	list<RedisCmdParaInfo> paraList;
	int32_t multiParaLen = 0;
	fillCommandPara("discard", 7, paraList);
	multiParaLen += 18;
	success = doTransactionCommandInConnection(multiParaLen, paraList, RedisCommandType::REDIS_COMMAND_DISCARD, con);
	freeCommandList(paraList);
	return success;
}


bool RedisClient::ExecTransaction(RedisConnection* con)
{
	if (m_redisMode != STAND_ALONE_OR_PROXY_MODE)
	{
		return false;
	}

	bool success = false;
	list<RedisCmdParaInfo> paraList;
	int32_t multiParaLen = 0;
	fillCommandPara("exec", 4, paraList);
	multiParaLen += 18;
	success = doTransactionCommandInConnection(multiParaLen, paraList, RedisCommandType::REDIS_COMMAND_EXEC, con);
	freeCommandList(paraList);
	return success;
}


bool RedisClient::FinishTransaction(RedisConnection** conn)
{
	if (m_redisMode != STAND_ALONE_OR_PROXY_MODE)
	{
		return false;
	}

	if (conn == NULL || *conn == NULL)
		return false;
	m_redisProxy.clusterHandler->releaseConnection(*conn);
	*conn = NULL;
	return true;
}


bool RedisClient::doTransactionCommandInConnection(int32_t commandLen, list<RedisCmdParaInfo>& commandList, int commandType, RedisConnection* con)
{
	RedisReplyInfo replyInfo;
	bool needRedirect;
	string redirectInfo;
	if (m_redisProxy.clusterHandler == NULL)
	{
		LOG_WRITE_ERROR("redisProxy clusterHandler is NULL");
		return false;
	}
	if (!m_redisProxy.clusterHandler->doRedisCommandOneConnection(commandList, commandLen, replyInfo, false, &con))
	{
		freeReplyInfo(replyInfo);

		std::stringstream log_msg;
		log_msg << "proxy:" << m_redisProxy.proxyId << " do redis command failed.";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}

	switch (commandType)
	{
		// expects "QUEUED"			
	case RedisCommandType::REDIS_COMMAND_SET:
	case RedisCommandType::REDIS_COMMAND_DEL:
	case RedisCommandType::REDIS_COMMAND_SADD:
	case RedisCommandType::REDIS_COMMAND_SREM:
		if (!parseQueuedResponseReply(replyInfo))
		{
			std::stringstream log_msg;
			log_msg << "parse queued command reply failed. reply string: " << replyInfo.resultString;
			LOG_WRITE_ERROR(log_msg.str());
			freeReplyInfo(replyInfo);
			return false;
		}
		break;
		// expects "OK"
	case RedisCommandType::REDIS_COMMAND_WATCH:
	case RedisCommandType::REDIS_COMMAND_UNWATCH:
	case RedisCommandType::REDIS_COMMAND_MULTI:
	case RedisCommandType::REDIS_COMMAND_DISCARD:
		if (!parseStatusResponseReply(replyInfo, needRedirect, redirectInfo))
		{
			std::stringstream log_msg;
			log_msg << "parse watch reply failed. reply string: " << replyInfo.resultString;
			LOG_WRITE_ERROR(log_msg.str());
			freeReplyInfo(replyInfo);
			return false;
		}
		break;
		// expects array
	case RedisCommandType::REDIS_COMMAND_EXEC:
		if (!parseExecReply(replyInfo))
		{
			std::stringstream log_msg;
			log_msg << "parse exec reply failed. reply string: " << replyInfo.resultString;
			LOG_WRITE_ERROR(log_msg.str());
			freeReplyInfo(replyInfo);
			return false;
		}
		break;
	default:
		return false;
	}
	freeReplyInfo(replyInfo);
	return true;
}

//for watch,unwatch,multi,discard command response.
bool RedisClient::parseStatusResponseReply(RedisReplyInfo& replyInfo)
{
	if (replyInfo.replyType != REDIS_REPLY_STATUS)
	{
		//		std::stringstream log_msg;
		//		log_msg<<"status response failed, redis response:["<<replyInfo.resultString<<"].";
		//		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	return true;
}

//for queued command in a transaction
bool RedisClient::parseQueuedResponseReply(RedisReplyInfo& replyInfo)
{
	if (replyInfo.replyType != REDIS_REPLY_STATUS)
	{
		return false;
	}
	return true;
}

bool RedisClient::parseExecReply(RedisReplyInfo& replyInfo)
{
	if (replyInfo.replyType == REDIS_REPLY_NIL)
	{
		LOG_WRITE_ERROR("transaction interrupted: nil");
		return false;
	}
	if (replyInfo.replyType != REDIS_REPLY_ARRAY)
	{
		std::stringstream log_msg;
		log_msg << "recv redis wrong reply type:[" << replyInfo.replyType << "].";
		LOG_WRITE_ERROR(log_msg.str());
		return false;
	}
	// empty array
	if (replyInfo.replyType == REDIS_REPLY_ARRAY && replyInfo.intValue == -1)
	{
		LOG_WRITE_ERROR("exec reply -1, set serial exec failed.");
		return false;
	}

	list<ReplyArrayInfo>::iterator arrayIter;
	for (arrayIter = replyInfo.arrayList.begin(); arrayIter != replyInfo.arrayList.end(); arrayIter++)
	{
		if ((*arrayIter).replyType == REDIS_REPLY_STRING)
		{
			// error type
			if (strncmp((*arrayIter).arrayValue, "-", 1) == 0)
			{
				std::stringstream log_msg;
				log_msg << "recv failed exec reply: " << (*arrayIter).arrayValue;
				LOG_WRITE_ERROR(log_msg.str());
				return false;
			}
			// integer type: 0
			else if (strncmp((*arrayIter).arrayValue, ":0", 2) == 0)
			{
				std::stringstream log_msg;
				log_msg << "recv failed exec reply: " << (*arrayIter).arrayValue;
				LOG_WRITE_WARNING(log_msg.str());
				return true;
			}
			// bulk string: nil
			else if (strncmp((*arrayIter).arrayValue, "$-1", 3) == 0)
			{
				std::stringstream log_msg;
				log_msg << "recv failed exec reply: " << (*arrayIter).arrayValue;
				LOG_WRITE_ERROR(log_msg.str());
				return false;
			}
			// array type: empty array
			else if (strncmp((*arrayIter).arrayValue, "*-1", 3) == 0)
			{
				std::stringstream log_msg;
				log_msg << "recv failed exec reply: " << (*arrayIter).arrayValue;
				LOG_WRITE_ERROR(log_msg.str());
				return false;
			}
		}
	}
	return true;
}


bool RedisClient::Set(RedisConnection* con, const string& key, const string& value)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("set", 3, paraList);
	paraLen += 15;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;
	fillCommandPara(value.c_str(), value.size(), paraList);
	paraLen += value.size() + 20;
	bool success = doTransactionCommandInConnection(paraLen, paraList, RedisCommandType::REDIS_COMMAND_SET, con);
	freeCommandList(paraList);
	return success;
}

bool RedisClient::Del(RedisConnection* con, const string& key)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("del", 3, paraList);
	paraLen += 15;
	fillCommandPara(key.c_str(), key.length(), paraList);
	paraLen += key.length() + 20;
	bool success = doTransactionCommandInConnection(paraLen, paraList, RedisCommandType::REDIS_COMMAND_DEL, con);
	freeCommandList(paraList);
	return success;
}

bool RedisClient::Sadd(RedisConnection* con, const string& setKey, const string& member)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("sadd", 4, paraList);
	paraLen += 15;
	fillCommandPara(setKey.c_str(), setKey.length(), paraList);
	paraLen += setKey.length() + 20;
	fillCommandPara(member.c_str(), member.size(), paraList);
	paraLen += member.size() + 20;
	bool success = doTransactionCommandInConnection(paraLen, paraList, RedisCommandType::REDIS_COMMAND_SADD, con);
	freeCommandList(paraList);
	return success;
}

bool RedisClient::Srem(RedisConnection* con, const string& setKey, const string& member)
{
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen = 0;
	fillCommandPara("srem", 4, paraList);
	paraLen += 15;
	fillCommandPara(setKey.c_str(), setKey.length(), paraList);
	paraLen += setKey.length() + 20;
	fillCommandPara(member.c_str(), member.size(), paraList);
	paraLen += member.size() + 20;
	bool success = doTransactionCommandInConnection(paraLen, paraList, RedisCommandType::REDIS_COMMAND_SREM, con);
	freeCommandList(paraList);
	return success;
}

static void PrintRedisReplyPre(RedisReply* reply, int depth=0)
{
	char buf[1024];
	int buflen=1024;

	RedisReply* r = static_cast<RedisReply*>(reply);
	switch (r->type)
	{
	case REDIS_REPLY_ERROR:
		snprintf(buf, buflen-1, "%*serr: %s", depth, "", r->str);
		LOG_WRITE_INFO(buf);
		break;
	case REDIS_REPLY_STATUS:
		snprintf(buf, buflen-1, "%*sstatus: %s", depth, "", r->str);
		LOG_WRITE_INFO(buf);
		break;
	case REDIS_REPLY_STRING:
		snprintf(buf, buflen-1, "%*sstring: %s", depth, "", r->str);
		LOG_WRITE_INFO(buf);
		break;
	case REDIS_REPLY_NIL:
		snprintf(buf, buflen-1, "%*snil", depth, "");
		LOG_WRITE_INFO(buf);
		break;
	case REDIS_REPLY_INTEGER:
		snprintf(buf, buflen-1, "%*sinteger: %lld", depth, "", r->integer);
		LOG_WRITE_INFO(buf);
		break;
	case REDIS_REPLY_ARRAY:
		snprintf(buf, buflen-1, "%*sarray: elements %zd", depth, "", r->elements);
		LOG_WRITE_INFO(buf);
		for (std::size_t i = 0; i < r->elements; ++i)
		{
			RedisReply* c = r->element[i];
			PrintRedisReplyPre(c, depth+2);
		}
		break;
	default:
		snprintf(buf, buflen-1, "error type: %d", r->type);
		LOG_WRITE_INFO(buf);
		break;
	}
	return;
}

bool RedisClient::TestHiredisGetReply()
{
	LOG_WRITE_INFO("TestHiredisGetReply");

	if(m_redisMode != SENTINEL_MODE)
		return false;

	RedisProxyInfo& masterNode = m_dataNodes[m_masterClusterId];
    if (masterNode.clusterHandler == NULL || masterNode.isAlived == false)
    {    
        std::stringstream log_msg;
        log_msg << "master node " << m_masterClusterId << " not alive";
        LOG_WRITE_ERROR(log_msg.str());
        if(!CreateConnectionPool(masterNode, m_passwd))
        {    
			LOG_WRITE_ERROR("CreateConnectionPool failed");
			return false;
        }    
    } 

	RedisConnection* con = masterNode.clusterHandler->getUnreleasedConnection();	
	if(con==false)
	{
		LOG_WRITE_INFO("cannot get connection");
		return false;
	}

	// 1 test with different command
	list<RedisCmdParaInfo> paraList;
	int32_t paraLen;
	RedisReply* reply=NULL;
	bool res;

	// 1.1 scan 0 COUNT 10
	fillScanCommandPara(0, "", 10, paraList, paraLen, SCAN_NOLOOP);
	res = con->doRedisCommand(paraList, paraLen, &reply);
	if(res)
	{
		PrintRedisReplyPre(reply, 0);		
		freeReplyObject(reply);
	}
	else
	{
		LOG_WRITE_ERROR("parse scan command reply failed");
	}
	freeCommandList(paraList);

	// 1.2 set

	// 1.3 get

	// 1.4 keys *	
	fillCommandPara("keys", 4, paraList);
    paraLen += 15;
    string key = "*"; 
    fillCommandPara(key.c_str(), key.length(), paraList);
    paraLen += key.length() + 20;
	res = con->doRedisCommand(paraList, paraLen, &reply);
	if(res)
    {
        PrintRedisReplyPre(reply, 0);
        freeReplyObject(reply);
    }
    else
    {
        LOG_WRITE_ERROR("parse scan command reply failed");
    }
    freeCommandList(paraList);

	return true;
}

} // namespace GBDownLinker 
