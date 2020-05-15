#include "redisbase.h"

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

bool parseRedisReply(char* replyString, RedisReplyInfo& replyInfo)
{
	if (replyString == NULL)
	{
		return false;
	}
	//parse first char.
	switch (replyString[0])
	{
	case '-':
		replyInfo.replyType = RedisReplyType::REDIS_REPLY_ERROR;
		break;
	case '+':
		replyInfo.replyType = RedisReplyType::REDIS_REPLY_STATUS;
		break;
	case ':':
		replyInfo.replyType = RedisReplyType::REDIS_REPLY_INTEGER;
		break;
	case '$':
		replyInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;
		break;
	case '*':
		replyInfo.replyType = RedisReplyType::REDIS_REPLY_ARRAY;
		break;
	default:
		replyInfo.replyType = RedisReplyType::REDIS_REPLY_UNKNOWN;
		return false;
	}
	//parse status and error string
	if (replyInfo.replyType == RedisReplyType::REDIS_REPLY_ERROR || replyInfo.replyType == RedisReplyType::REDIS_REPLY_STATUS)
	{
		char* p = strstr(replyString, "\r\n");
		if (p == NULL)
		{
			return false;
		}
		char buf[4096];
		memset(buf, 0, 4096);
		memcpy(buf, replyString + 1, p - replyString - 1);
		replyInfo.resultString = buf;
		return true;
	}
	if (replyInfo.replyType == RedisReplyType::REDIS_REPLY_INTEGER)
	{
		char* p = strstr(replyString, "\r\n");
		if (p == NULL)
		{
			return false;
		}
		char buf[64];
		memset(buf, 0, 64);
		memcpy(buf, replyString + 1, p - replyString - 1);
		replyInfo.intValue = atoi(buf);
		return true;
	}
	bool success;
	if (replyInfo.replyType == RedisReplyType::REDIS_REPLY_STRING)
	{
		ReplyArrayInfo arrayInfo;
		success = parseArrayInfo(&replyString, arrayInfo);
		if (success)
			replyInfo.arrayList.push_back(arrayInfo);
		return success;
	}
	if (replyInfo.replyType == RedisReplyType::REDIS_REPLY_ARRAY)
	{
		//first check array num
		char* p = strstr(replyString, "\r\n");
		if (p == NULL)
		{
			return false;
		}
		char buf[64];
		memset(buf, 0, 64);
		memcpy(buf, replyString + 1, p - replyString - 1);
		int arrayNum = atoi(buf);
		for (int i = 0; i < arrayNum; i++)
		{
			char* arrayString = p + 2;
			ReplyArrayInfo arrayInfo;
			success = parseArrayInfo(&arrayString, arrayInfo);
			if (success)
				replyInfo.arrayList.push_back(arrayInfo);
			else
				return false;
		}
	}
	return true;
}

bool parseArrayInfo(char** arrayString, ReplyArrayInfo& arrayInfo)
{
	char* string = *arrayString;
	if (strncmp(string, "$", 1) != 0)
	{
		return false;
	}
	char* p = strstr(string, "\r\n");
	if (p == NULL)
	{
		return false;
	}
	//first parse length
	char buf[128];
	memset(buf, 0, 128);
	memcpy(buf, string + 1, p - string - 1);
	arrayInfo.arrayLen = atoi(buf);
	if (arrayInfo.arrayLen == -1)
	{
		arrayInfo.replyType = RedisReplyType::REDIS_REPLY_NIL;
		*arrayString = p + 2;
		return true;
	}
	arrayInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;
	arrayInfo.arrayValue = (char*)malloc(arrayInfo.arrayLen);
	memcpy(arrayInfo.arrayValue, p + 2, arrayInfo.arrayLen);
	*arrayString = p + 2 + arrayInfo.arrayLen + 2;
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

} // namespace GBDownLinker
