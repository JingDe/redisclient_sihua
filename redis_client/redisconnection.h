#ifndef REDISCONNECTION_H
#define REDISCONNECTION_H

#include <string>
#include <queue>

#include "socket.h"
#include "redisbase.h"
#include "redis_client_util.h"

using namespace std;

namespace GBDownLinker {

#define REDIS_READ_BUFF_SIZE 4096

class RedisConnection
{
public:
	enum RedisParseState {
		REDIS_PARSE_UNKNOWN_STATE = 0,
		REDIS_PARSE_TYPE,
		REDIS_PARSE_LENGTH,
		REDIS_PARSE_ARRAYLENGTH,
		REDIS_PARSE_INTEGER,	// 4
		REDIS_PARSE_RESULT,
		REDIS_PARSE_STRING,

		// for parseScanReply
		REDIS_PARSE_CURSORLEN,	// 7
		REDIS_PARSE_CURSOR,
		REDIS_PARSE_KEYSLEN,
		REDIS_PARSE_KEYLEN,
		REDIS_PARSE_KEY,

		// for parseEnhance
		REDIS_PARSE_CHECK_ARRAYS_SIZE,  // 等待确定第二行是否*开头
		REDIS_PARSE_RESULT_OK,			// 区分 +OK 和 -ERR
		REDIS_PARSE_RESULT_ERR,

	};
	enum ReplyParserType {
		COMMON_PARSER,
		SCAN_PARSER,
	};
	RedisConnection(const string serverIp, uint32_t serverPort, uint32_t connectTimeout, uint32_t readTimeout, const std::string& passwd);
	~RedisConnection();
	bool connect();
	//	bool doRedisCommand(list<RedisCmdParaInfo> &paraList, int32_t paraLen, RedisReplyInfo& replyInfo, ParseFunction parser=NULL);
	bool doRedisCommand(list<RedisCmdParaInfo>& paraList, int32_t paraLen, RedisReplyInfo& replyInfo, ReplyParserType parserType = COMMON_PARSER);
	bool close();
	bool recv(RedisReplyInfo& replyInfo, ReplyParserType parserType = COMMON_PARSER);

	string toString() {
		string desc = "server ip=" + m_serverIp + ", port=" + toStr(m_serverPort);
		return desc;
	}

	bool CanRelease()
	{
		return m_canRelease;
	}
	void SetCanRelease(bool canRelease);

	string GetServerIp() {
		return m_serverIp;
	}
	uint32_t GetServerPort() {
		return m_serverPort;
	}
	
	ConnectionStatus CheckConnected();

	bool doCommandWithParseEnhance(list < RedisCmdParaInfo >& paraList, int32_t paraLen, CommonReplyInfo& replyInfo);
	bool recvWithParseEnhance(CommonReplyInfo& replyInfo);

	int GetSockfd();

private:
	bool send(char* request, uint32_t sendLen);
	void checkConnectionStatus();
	bool parse(char* parseBuf, int32_t parseLen, RedisReplyInfo& replyInfo);
	bool parseScanReply(char* parseBuf, int32_t parseLen, RedisReplyInfo& replyInfo);

	bool parseEnhance(char* parseBuf, int32_t parseLen, CommonReplyInfo& replyInfo);
	//	bool parseEnhance2(char *parseBuf, int32_t parseLen, CommonReplyInfo2 & replyInfo);
	bool AuthPasswd();

public:
	bool m_available;
	//record connection connect time.
	uint32_t m_connectTime;
private:
	string m_serverIp;
	uint32_t m_serverPort;
	Socket m_socket;
	char* m_unparseBuf;
	int32_t  m_unparseLen;
	int32_t  m_parseState;
	int32_t  m_arrayNum;
	int32_t  m_doneNum;
	int32_t  m_arrayLen;
	bool   	m_valid;
	bool 	m_canRelease;
	int m_connectTimeout; // milliSeconds
	int m_readTimeout; // milliseconds
	std::string m_passwd;
};

}

#endif
