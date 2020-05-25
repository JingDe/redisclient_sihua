
#include"hiredis.h"


class FakeRedisServer {


};

// 先根遍历打印
void printReplyObjectPre(void* reply, int depth=0)
{
	redisReply* r = static_cast<redisReply*>(reply);
	switch (r->type)
	{
	case REDIS_REPLY_ERROR:
		printf("%*serr: %s\n", depth, "", r->str);
		break;
	case REDIS_REPLY_STATUS:
		printf("%*sstatus: %s\n", depth, "", r->str);
		break;
	case REDIS_REPLY_STRING:
		printf("%*sstring: %s\n", depth, "", r->str);
		break;
	case REDIS_REPLY_NIL:
		printf("%*snil\n", depth, "");
		break;
	case REDIS_REPLY_INTEGER:
		printf("%*sinteger: %lld\n", depth, "", r->integer);
		break;
	case REDIS_REPLY_ARRAY:
		printf("%*sarray: elements %lld\n", depth, "", r->elements);
		for (std::size_t i = 0; i < r->elements; ++i)
		{
			redisReply* c = r->element[i];
			printReplyObjectPre(c, depth+2);
		}
		break;
	default:
		printf("error type: %d\n", r->type);
		break;
	}
	return;
}

// 后跟遍历打印
void printReplyObjectPost(void* reply)
{
	redisReply* r = static_cast<redisReply*>(reply);
	switch (r->type)
	{
	case REDIS_REPLY_ERROR:
		printf("err: %s\n", r->str);
		break;
	case REDIS_REPLY_STATUS:
		printf("status: %s\n", r->str);
		break;
	case REDIS_REPLY_STRING:
		printf("string: %s\n", r->str);
		break;
	case REDIS_REPLY_NIL:
		printf("nil\n");
		break;
	case REDIS_REPLY_INTEGER:
		printf("integer: %lld\n", r->integer);
		break;
	case REDIS_REPLY_ARRAY:		
		for (std::size_t i = 0; i < r->elements; ++i)
		{
			redisReply* c = r->element[i];
			printReplyObjectPost(c);
		}
		printf("array: elements %lld\n", r->elements);
		break;
	default:
		printf("error type: %d\n", r->type);
		break;
	}
	return;
}


int main(int argc, char** argv)
{
	if (argc < 2)
	{
		printf("usage %s filename\n", argv[0]);
		return -1;
	}
	std::string replyfile = argv[1];
	printf("reply stored in file %s\n", replyfile.c_str());

	redisContext rc;
	if (!redisContextInit(replyfile, &rc))
	{
		redisContextUninit(&rc);
		return -1;
	}

	void* reply;
	int result = redisGetReply(&rc, &reply);
	if (result == REDIS_OK)
	{
		printf("***********************\npre order\n***********************\n");
		printReplyObjectPre(reply);

		printf("***********************\npost order\n***********************\n");
		printReplyObjectPost(reply);
	}
	else
	{
		printf("parse failed\n");
	}

	freeReplyObject(reply);
	redisContextUninit(&rc);

	return 0;
}