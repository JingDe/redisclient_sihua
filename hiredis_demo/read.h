#ifndef HIREDIS_READ_H
#define HIREDIS_READ_H

#include<cstdlib>
#include<string>

#define REDIS_READER_MAX_BUF (1024*16)  /* Default max unused reader buffer. */
#define REDIS_READER_STACK_SIZE 9

enum RedisReplyType {
	REDIS_REPLY_ERROR = 1,
	REDIS_REPLY_STATUS,
	REDIS_REPLY_NIL, // 长度为-1的节点
	REDIS_REPLY_INTEGER,
	REDIS_REPLY_STRING,
	REDIS_REPLY_ARRAY,
};


typedef struct redisReply {
	int type; // 回复类型
	long long integer;
	double dval;
	size_t len;
	char* str;

	std::size_t elements;
	struct redisReply** element;
} redisReply;

typedef struct redisReadTask {
	int type; // 当前解析节点对应回复节点的类型
	int elements; // 子节点个数
	int idx; // 当前节点在父节点儿子节点中的索引
	void* obj; // 对应的回复节点
	struct redisReadTask* parent;
	void* privdata;
} redisReadTask;

typedef struct redisReplyObjectFunctions {
	void* (*createString)(const redisReadTask*, char*, size_t);
	void* (*createArray)(const redisReadTask*, size_t);
	void* (*createInteger)(const redisReadTask*, long long);
	void* (*createNil)(const redisReadTask*);

	void (*freeObject)(void*);
} redisReplyObjectFunctions;

typedef struct redisReader {
	int err;
	char errstr[128];
	
	// sds
	char* buf; // 存储reply的输入缓存
	size_t pos; // 当前解析位置
	size_t len;
	size_t maxbuf; // 允许缓存中的最大空闲大小，超出时删除已解析部分数据

	redisReadTask** task;
	int tasks; // task的长度
	int ridx; // 当前解析到第几层
	void* reply; // 存储最终解析结果的根节点?

	redisReplyObjectFunctions* fn; // 解析基本类型的方法
	void* privdata;
} redisReader;


std::string desc(int type);


redisReader* redisReaderCreateWithFunctions(redisReplyObjectFunctions* fn);
void redisReaderFree(redisReader* r);

char* seekNewline(char* s, size_t len);
char* readLine(redisReader* r, int* _len);

int processItem(redisReader* r);
void moveToNextTask(redisReader* r);
int processLineItem(redisReader* r);
int processMultiBulkItem(redisReader* r);
int processBulkItem(redisReader* r);


#endif