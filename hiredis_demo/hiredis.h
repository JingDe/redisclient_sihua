#ifndef HIREDIS_H
#define HIREDIS_H

#include"read.h"

#include<string>

// ��redis���ӵ�������, from hiredisԴ��
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


// ���ԣ����ļ��ж�ȡreply����
//typedef struct fakeRedisContext {
//	struct redisContex* base; // "�̳�" redisContext
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

	// ���ڲ���
	std::string replyfile; // ����������ݣ�redis reply���ݵ��ļ�
	int fd; // �ļ����

	ssize_t read(char* buf, size_t len); // ��fd�ж�ȡlen��С���ݵ�����buf�У�����ʵ�ʶ�ȡ�ֽ���

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