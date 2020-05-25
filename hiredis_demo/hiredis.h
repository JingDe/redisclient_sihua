#ifndef HIREDIS_H
#define HIREDIS_H

#include"read.h"

#include<string>

// 到redis连接的上下文, from hiredis源码
//typedef struct redisContex {
//	const redisContextFuncs* funcs;
//
//	int err;
//	char errstr[128];
//	redisFD fd;
//	int flags;
//	char* obuf;
//	redisReader* reader;
//
//	enum redisConnectionType connection_type;
//	struct timeval* timeout;
//
//	struct {
//		char* host;
//		char* soucr_addr;
//		int port;
//	} tcp;
//
//	struct {
//		char* path;
//	} unix_sock;
//
//	struct sockadr* saddr;
//	size_t addrlen;
//	
//	void* privdata;
//} redisContext;


// 测试，从文件中读取reply数据
//typedef struct fakeRedisContext {
//	struct redisContex* base; // "继承" redisContext
//
//	std::string replyFile;
//	int fd;
//
//	redisReader* reader;
//	int err;
//
//	void* (*read)(char*, size_t);
//};

typedef struct redisContext {

	int err;
	redisReader* reader;

	// 用于测试
	std::string replyfile; // 保存测试数据：redis reply数据的文件
	int fd; // 文件句柄

	ssize_t read(char* buf, size_t len); // 从fd中读取len大小数据到缓存buf中，返回实际读取字节数

} redisContext;

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

int redisGetReply(redisContext* c, void** reply);
int redisBufferRead(redisContext* c);
int redisReaderFeed(redisReader* r, const char* buf, size_t len);
int redisReaderGetReply(redisReader* r, void** reply);


bool redisContextInit(const std::string& filename, redisContext* rc);
bool redisContextUninit(redisContext* rc);

void freeReplyObject(void* reply);

#endif