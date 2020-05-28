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
	char* buf; // 存储reply的输入缓存
	size_t pos; // 当前解析位置
	size_t len;
	size_t maxbuf; // 允许缓存中的最大空闲大小，超出时删除已解析部分数据

	RedisReadTask** task;
	int tasks; // task的长度
	int ridx; // 当前解析到第几层
	int height; // 解析树的高度
	RedisReply* reply; // 存储最终解析结果的根节点?

	//RedisReplyObjectFunctions* fn; // 解析基本类型的方法
	void* privdata;
};

}

#endif
