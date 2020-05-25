#ifndef HIREDIS_READ_H
#define HIREDIS_READ_H

#include<cstdlib>
#include<string>

#define REDIS_READER_MAX_BUF (1024*16)  /* Default max unused reader buffer. */
#define REDIS_READER_STACK_SIZE 9

enum RedisReplyType {
	REDIS_REPLY_ERROR = 1,
	REDIS_REPLY_STATUS,
	REDIS_REPLY_NIL, // ����Ϊ-1�Ľڵ�
	REDIS_REPLY_INTEGER,
	REDIS_REPLY_STRING,
	REDIS_REPLY_ARRAY,
};


typedef struct redisReply {
	int type; // �ظ�����
	long long integer;
	double dval;
	size_t len;
	char* str;

	std::size_t elements;
	struct redisReply** element;
} redisReply;

typedef struct redisReadTask {
	int type; // ��ǰ�����ڵ��Ӧ�ظ��ڵ������
	int elements; // �ӽڵ����
	int idx; // ��ǰ�ڵ��ڸ��ڵ���ӽڵ��е�����
	void* obj; // ��Ӧ�Ļظ��ڵ�
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
	char* buf; // �洢reply�����뻺��
	size_t pos; // ��ǰ����λ��
	size_t len;
	size_t maxbuf; // �������е������д�С������ʱɾ���ѽ�����������

	redisReadTask** task;
	int tasks; // task�ĳ���
	int ridx; // ��ǰ�������ڼ���
	void* reply; // �洢���ս�������ĸ��ڵ�?

	redisReplyObjectFunctions* fn; // �����������͵ķ���
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