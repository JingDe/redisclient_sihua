#ifndef REDISBASE_H
#define REDISBASE_H

#include <string>
#include <list>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include <bitset>
#include <string.h>
#include <stdlib.h>
#include <malloc.h>
#include <ostream>

using namespace std;

namespace GBDownLinker {

enum RedisStatus {
    REDIS_OK=0,
    REDIS_ERR=-1,
};


enum {
	REDIS_REPLY_UNKNOWN = 0,
	REDIS_REPLY_NIL,		// 1
	REDIS_REPLY_ERROR,		// 2
	REDIS_REPLY_STATUS,		// 3
	REDIS_REPLY_INTEGER,	// 4
	REDIS_REPLY_STRING,		// 5
	REDIS_REPLY_ARRAY, 		// 6
};

typedef struct RedisCmdParaInfoTag
{
	//need release paravalue mem after used
	RedisCmdParaInfoTag()
	{
		paraValue = NULL;
		paraLen = 0;
	}
	char* paraValue;
	int32_t paraLen;
}RedisCmdParaInfo;

typedef struct ReplyArrayInfoTag
{
	//need release mem by user.
	ReplyArrayInfoTag()
	{
		replyType = REDIS_REPLY_UNKNOWN;
		arrayValue = NULL;
		arrayLen = 0;
	}
	int replyType;
	char* arrayValue;
	int32_t arrayLen;
}ReplyArrayInfo;

typedef struct RedisReplyInfoTag
{
	RedisReplyInfoTag()
	{
		replyType = REDIS_REPLY_UNKNOWN;
		resultString.clear();
		intValue = 0;
		arrayList.clear();
	}
	int replyType;
	string resultString;
	int intValue;
	list<ReplyArrayInfo> arrayList;
}RedisReplyInfo;

// 参考 hiredis
typedef struct RedisReply {
	int type; // 回复类型
	long long integer;
	double dval;
	size_t len; // 
	char* str;

	std::size_t elements;
	struct RedisReply** element;
} RedisReply;

typedef struct RedisReadTask {
	int type; // 当前解析节点对应回复节点的类型
	int elements; // 子节点个数
	int idx; // 当前节点在父节点儿子节点中的索引
	void* obj; // 对应的回复节点
	struct RedisReadTask* parent;
	void* privdata;
} RedisReadTask;

// 创建和释放RedisReply节点的函数，参考hiredis
typedef struct RedisReplyObjectFunctions {
	RedisReply* (*createString)(const RedisReadTask*, char*, size_t);
	RedisReply* (*createArray)(const RedisReadTask*, size_t);
	RedisReply* (*createInteger)(const RedisReadTask*, long long);
	RedisReply* (*createNil)(const RedisReadTask*);

	void (*freeObject)(RedisReply*);
} RedisReplyObjectFunctions;

RedisReply* createReplyObject(int type);
RedisReply* createIntegerObject(const RedisReadTask* task, long long value);
RedisReply* createNilObject(const RedisReadTask* task);
RedisReply* createStringObject(const RedisReadTask* task, char* str, size_t len);
RedisReply* createArrayObject(const RedisReadTask* task, size_t elements);
void freeReplyObject(RedisReply* reply);

enum class DoRedisCmdResultType : uint8_t {
	Success=0,
	Disconnected,
	NoAuth,
	Fail,
	NotFound,
	Redirected,
	InternalError,
	UnknownError,
};

enum RedisMode {
	STAND_ALONE_OR_PROXY_MODE,
	CLUSTER_MODE,
	SENTINEL_MODE,
};

enum class ConnectionStatus : uint8_t {
    Connected=0,
    Reconnected,
    Disconnected,
};


enum class WaitReadEventResult : uint8_t {
	Readable=0,
	Timeout,
	Disconnected,
	InternalError,
};

std::ostream& operator<<(std::ostream& os, WaitReadEventResult result);

bool createRedisCommand(list<RedisCmdParaInfo>& paraList, char** cmdString, int32_t& cmdLen);


} // namespace GBDownLinker

#endif

