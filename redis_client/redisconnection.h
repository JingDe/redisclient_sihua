#ifndef REDISCONNECTION_H
#define REDISCONNECTION_H

#include <string>
#include <queue>

#include "socket.h"
#include "redisbase.h"
#include "redis_client_util.h"
#include "redis_reader.h"

using namespace std;

namespace GBDownLinker {

enum RedisStatus {
    REDIS_OK=0,
    REDIS_ERR=-1,
};

enum RedisErrorType {
    REDIS_ERR_OK=0,
    REDIS_ERR_OOM,
    REDIS_ERR_PROTOCOL,
    REDIS_ERR_DEBUG,
};

#define REDIS_READ_BUFF_SIZE 4096
#define REDIS_READER_STACK_SIZE 9
#define REDIS_READER_MAX_BUF (1024*16)  /* Default max unused reader buffer. */

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
        REDIS_PARSE_CURSORLEN,  // 7                                                                  
        REDIS_PARSE_CURSOR,
        REDIS_PARSE_KEYSLEN,
        REDIS_PARSE_KEYLEN,
        REDIS_PARSE_KEY,
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
	bool doRedisCommand(list < RedisCmdParaInfo >& paraList, int32_t paraLen, RedisReply& reply);
	
	bool close();
	
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
	
	bool recv(RedisReplyInfo& replyInfo, ReplyParserType parserType = COMMON_PARSER);
	ConnectionStatus CheckConnected();

	int GetSockfd();
	bool AuthPasswd();

private:
	bool send(char* request, uint32_t sendLen);
	void checkConnectionStatus();
	bool parse(char* parseBuf, int32_t parseLen, RedisReplyInfo& replyInfo);
	bool parseScanReply(char* parseBuf, int32_t parseLen, RedisReplyInfo& replyInfo);
	
	bool resetReader();
	bool initReader();
	bool freeReader();

    bool getReply(RedisReply& reply);
    int redisBufferRead();
    int readReply(char*, size_t);
    int RedisReaderGetReply(RedisReader*, RedisReply**);
    int RedisReaderGrow(RedisReader* r);
    int RedisReaderFeed(RedisReader* r, const char* buf, size_t len);

    char* readLine(RedisReader* r, int* len);
    char* readBytes(RedisReader* r, unsigned int bytes);
    char* seekNewline(char* s, size_t len);

	int processItem(RedisReader* r);
	int processLineItem(RedisReader* r);
	int processBulkItem(RedisReader* r);
	int processMultiBulkItem(RedisReader* r);
	void moveToNextTask(RedisReader* r);

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

	RedisReader* m_reader;
	static RedisReplyObjectFunctions defaultFunctions;
};

}

#endif
