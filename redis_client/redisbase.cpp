#include "redisbase.h"

#include<cassert>

namespace GBDownLinker {

bool createRedisCommand(list < RedisCmdParaInfo >& paraList, char** cmdString, int32_t& cmdLen)
{
	//first check cmd string
	if (*cmdString == NULL)
	{
		return false;
	}
	if (paraList.empty())
	{
		return false;
	}
	int argNum = paraList.size();
	char paraBuf[4096];
	memset(paraBuf, 0, 4096);
	sprintf(paraBuf, "*%d\r\n", argNum);

	cmdLen = 0;
	memcpy(*cmdString + cmdLen, paraBuf, strlen(paraBuf));
	cmdLen += strlen(paraBuf);
	list<RedisCmdParaInfo>::iterator paraIter;
	//free list info by who malloc it.
	for (paraIter = paraList.begin(); paraIter != paraList.end(); paraIter++)
	{
		RedisCmdParaInfo paraInfo = (*paraIter);
		memset(paraBuf, 0, 4096);
		sprintf(paraBuf, "$%d\r\n", paraInfo.paraLen);
		memcpy(*cmdString + cmdLen, paraBuf, strlen(paraBuf));
		cmdLen += strlen(paraBuf);
		memcpy(*cmdString + cmdLen, paraInfo.paraValue, paraInfo.paraLen);
		cmdLen += paraInfo.paraLen;
		memcpy(*cmdString + cmdLen, "\r\n", 2);
		cmdLen += 2;
	}
	return true;
}

std::ostream& operator<<(std::ostream& os, WaitReadEventResult result)
{
	switch(result)
	{
	case WaitReadEventResult::Readable:
		os<<"Readable";
		break;
	case WaitReadEventResult::Timeout:
		os<<"Timeout";
		break;
	case WaitReadEventResult::InternalError:
		os<<"InternalError";
		break;
	case WaitReadEventResult::Disconnected:
		os<<"Disconnected";
		break;
	}
	return os;
}

RedisReply* createReplyObject(int type)
{
	RedisReply* r = (RedisReply*)calloc(1, sizeof(*r));

	if (r == NULL)
		return NULL;

	//	LOG_DEBUG("DEBUG createReplyObject type: %d pointer: %p\n", type, r);

	r->type = type;
	return r;
}


void freeReplyObject(RedisReply* reply)
{
	//LOG_DEBUG("freeReplyObject %p\n", reply);

	RedisReply* r = static_cast<RedisReply*>(reply);
	std::size_t j;

	if (r == NULL)
		return;

	switch (r->type)
	{
	case REDIS_REPLY_INTEGER:
	case REDIS_REPLY_NIL:
		break;
	case REDIS_REPLY_ARRAY:
		if (r->element != NULL)
		{
			for (j = 0; j < r->elements; j++)
			{
				freeReplyObject(r->element[j]);
			}
			free(r->element);
		}
		break;
	case REDIS_REPLY_STATUS:
	case REDIS_REPLY_ERROR:
	case REDIS_REPLY_STRING:
		free(r->str);
		break;
	}
	free(r);
}

RedisReply* createIntegerObject(const RedisReadTask* task, long long value)
{
	RedisReply* r, * parent;

	r = createReplyObject(REDIS_REPLY_INTEGER);
	if (r == NULL)
		return NULL;

	r->integer = value;
	if (task->parent)
	{
		parent = static_cast<RedisReply*>(task->parent->obj);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}
	return r;
}

RedisReply* createNilObject(const RedisReadTask* task)
{
	RedisReply* r, * parent;

	r = createReplyObject(REDIS_REPLY_NIL);
	if (r == NULL)
		return NULL;

	if (task->parent)
	{
		parent = static_cast<RedisReply*>(task->parent->obj);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}
	return r;
}

RedisReply* createStringObject(const RedisReadTask* task, char* str, size_t len)
{
	RedisReply* r, * parent;
	char* buf;

	r = createReplyObject(task->type);
	if (r == NULL)
		return NULL;

	buf = (char*)malloc(len + 1);
	if (buf == NULL)
	{
		freeReplyObject(r);
		return NULL;
	}

	assert(task->type == REDIS_REPLY_ERROR || task->type == REDIS_REPLY_STATUS || task->type == REDIS_REPLY_STRING);

	memcpy(buf, str, len);
	buf[len] = '\0';
	r->str = buf;
	r->len = len;

	if (task->parent)
	{
		parent = static_cast<RedisReply*>(task->parent->obj);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}
	return r;
}

RedisReply* createArrayObject(const RedisReadTask* task, size_t elements)
{
	RedisReply* r, * parent;

	r = createReplyObject(task->type);
	if (r == NULL)
		return NULL;

	if (elements > 0)
	{
		r->element = reinterpret_cast<RedisReply**>(calloc(elements, sizeof(RedisReply*)));
		if (r->element == NULL)
		{
			freeReplyObject(r);
			return NULL;
		}
	}

	r->elements = elements;

	if (task->parent)
	{
		parent = static_cast<RedisReply*>(task->parent->obj);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}
	return r;
}

} // namespace GBDownLinker
