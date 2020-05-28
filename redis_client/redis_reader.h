#ifndef REDIS_READER_H
#define REDIS_READER_H

#include"redisbase.h"


namespace GBDownLinker {

struct RedisReadTask;
struct RedisReply;

struct RedisReader {

	int err;
	char errstr[128];

	// sds
	char* buf; // �洢reply�����뻺��
	size_t pos; // ��ǰ����λ��
	size_t len;
	size_t maxbuf; // �������е������д�С������ʱɾ���ѽ�����������

	RedisReadTask** task;
	int tasks; // task�ĳ���
	int ridx; // ��ǰ�������ڼ���
	int height; // �������ĸ߶�
	RedisReply* reply; // �洢���ս�������ĸ��ڵ�?

	//RedisReplyObjectFunctions* fn; // �����������͵ķ���
	void* privdata;
};

}

#endif
