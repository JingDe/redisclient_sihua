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

struct RedisReplyType
{
	enum {
		REDIS_REPLY_UNKNOWN = 0,
		REDIS_REPLY_NIL,
		REDIS_REPLY_ERROR,
		REDIS_REPLY_STATUS,
		REDIS_REPLY_INTEGER,
		REDIS_REPLY_STRING, // 5
		REDIS_REPLY_ARRAY, 	// 6

		// TODO for parseEnance
		REDIS_REPLY_MULTI_ARRRY,
	};
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
		replyType = RedisReplyType::REDIS_REPLY_UNKNOWN;
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
		replyType = RedisReplyType::REDIS_REPLY_UNKNOWN;
		resultString.clear();
		intValue = 0;
		arrayList.clear();
	}
	int replyType;
	string resultString;
	int intValue;
	list<ReplyArrayInfo> arrayList;
}RedisReplyInfo;


/* for parseEnhanch, 更通用、全面的解析resp协议reply */

// 沿用 ReplyArrayInfo
//struct ArrayElem{
//	int elem_type;
//	string elem_value;
//};
//typedef vector<ArrayElem> Array;
typedef vector<ReplyArrayInfo> Array;

//struct ReplyInfo{
//	// 返回一条成功或者失败消息
//	// 返回一个整数的命令
//	// 返回一个对象，一个string对象。get
//	// 返回一个数组。 smembers, sentinel master,
//	// 返回一个整数，和一个数组。  scan
//	// 返回多个数组。 sentinel salves,
//	
//	int code; // 成功或者失败
//	string msg; // 失败msg
//	int num; // 
//	string entity; // string表示的对象
//	vector<Array> arrays;
//
//	// 记录parse过程中的中间状态
//	int arrays_size; // 获取arrays的大小
//	int cur_array_pos; // 当前解析的第几个Array
//	int cur_array_size; // 获取当前解析的Array的大小
//};

struct CommonReplyInfo {
	int replyType;
	string resultString;
	int intValue;

	//	list<ReplyArrayInfo> arrayList; // 返回一个string对象时，存储在第一个元素
	vector<Array> arrays; // 返回一个string对象时，存储在第一个Array的第一个元素

	// 记录parse过程中的中间状态
	int arrays_size; // 获取arrays的大小
	int cur_array_pos; // 当前解析的第几个Array
	int cur_array_size; // 获取当前解析的Array的大小
};

struct CommonReplyInfo2 {
	int replyType;
	string resultString;
	int intValue;

	// 方法一：
//	 vector<void*> arrays;
//	 arrays存储的元素类型可以不同，可以是int*, string*, vector<ReplyArrayInfo>*
//	 不可行，原因：存储到array之后不知道原始类型是什么指针.
//	struct ElemType{
//		int pointerType;
//		void* value;
//	};
//	vector<ElemType*> arrays;


	// 方法二：
	// vector<vector<void*> > arrays; 
	// int*,string*存储在vector<void*> arrays[x]中
	// vector<void*> arrays[x], 也可以作为vector<ReplyArrayInfo*>类型
	// 问题：不能确定void*真正类型。
	struct ElemType1 {
		int pointerType;
		vector<void*> vec;
	};
	vector<ElemType1> arrays;

	// 或者
//	struct ElemType2{
//		int pointerType;
//		void* value;
//	}; 
//	vector<vector<ElemType2> > arrays;


	// 记录parse过程中的中间状态
	int arrays_size; // 获取arrays的大小
	int cur_array_pos; // 当前解析的第几个Array
	int cur_array_size; // 获取当前解析的Array的大小
};

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
bool parseRedisReply(char* replyString, RedisReplyInfo& replyInfo);
bool parseArrayInfo(char** arrayString, ReplyArrayInfo& arrayInfo);

} // namespace GBDownLinker

#endif

