

/*

redisReply： 以树的结构表示解析好的redis回复信息。一个RedisReply表示树中的一个节点。


redisReader: 解析redis回复的解析器
	存储redis回复的原始数据
	解析状态：当前解析到树的第几层，当前节点是其父节点的第几个儿子节点。
	创建基本类型的回复节点redisReply的方法。


redisReadTask：
	用于解析树中的某一个节点。记录当前节点的类型，儿子节点的个数，在父节点儿子中的索引，指向父节点redisReaderTask的指针。

	redisReadTask节点，以多叉树的结构，一一对应最终解析出的 RedisReply树中的每一个节点。

	通过redisRedisTask，以先根遍历的顺序，构造redisReply树。


完整的解析流程分析：
	开辟一个缓冲区，尽可能的读取redis的回复数据。
	初始时，从根节点开始解析，

*/

#include"read.h"
#include"sds.h"
#include"hiredis.h"
#include"util.h"

#include<cassert>
#include<cstdint>
#include<climits>

//#define SIZE_MAX ULONG_MAX


std::string desc(int type)
{
	switch (type)
	{
	case REDIS_REPLY_ERROR:
		return "error type";
	case REDIS_REPLY_STATUS:
		return "status type";
	case REDIS_REPLY_NIL:
		return "nil type";
	case REDIS_REPLY_INTEGER:
		return "integer type";
	case REDIS_REPLY_STRING:
		return "string type";
	case REDIS_REPLY_ARRAY:
		return "array type";
	default:
		return "error type";
	}
	assert(false);
}

// 从s开始，寻找\r\n，返回指向\r的指针
char* seekNewline(char* s, size_t len)
{
	int pos = 0;
	int _len = len - 1;

	while (pos < _len)
	{
		while (pos < _len && s[pos] != '\r')
			pos++;
		if (pos == _len)
		{
			return NULL;
		}
		else
		{
			if (s[pos + 1] == '\n')
			{
				return s + pos;
			}
			else
			{
				pos++;
			}
		}
	}
	return NULL;
}

char* readLine(redisReader* r, int* _len)
{
	char* p, * s;
	int len;

	p = r->buf + r->pos;
	s = seekNewline(p, (r->len - r->pos));
	if (s != NULL)
	{
		len = s - (r->buf + r->pos);
		r->pos += len + 2; // 指向下一行的开始位置
		if (_len)
			*_len = len; // 返回当前行的长度
		return p; // 返回行首地址
	}
	return NULL;
}

char* readBytes(redisReader* r, unsigned int bytes)
{
	char* p=NULL;
	if (r->len - r->pos >= bytes)
	{
		p = r->buf + r->pos;
		r->pos += bytes;
		return p;
	}
	return NULL;
}

// ----------------------------------------------------------------------

int redisReaderGrow(redisReader* r)
{
	redisReadTask** aux;
	int newlen;

	newlen = r->tasks + REDIS_READER_STACK_SIZE;
	aux = static_cast<redisReadTask**>(realloc(r->task, sizeof(*r->task) * newlen));
	if (aux == NULL)
		goto oom;

	r->task = aux;

	for (; r->tasks < newlen; r->tasks++)
	{
		r->task[r->tasks] = static_cast<redisReadTask*>(calloc(1, sizeof(**r->task)));
		if (r->task[r->tasks] == NULL)
			goto oom;
	}

	return REDIS_OK;

oom:
	r->err = REDIS_ERR_PROTOCOL;
	return REDIS_ERR;
}


int processLineItem(redisReader* r)
{
	redisReadTask* cur = r->task[r->ridx];
	void* obj;
	char* p;
	int len;

    if(r->ridx > r->height)
    {   
        r->height = r->ridx;
    } 

	if ((p = readLine(r, &len)) != NULL)
	{
		if (cur->type == REDIS_REPLY_INTEGER)
		{
			if (r->fn && r->fn->createInteger)
			{
				long long v;
				if (string2ll_norah(p, len, &v) == REDIS_ERR)
				{
					r->err = REDIS_ERR_PROTOCOL;
					return REDIS_ERR;
				}
				obj = r->fn->createInteger(cur, v);
			}
			else
			{
				obj = (void*)REDIS_REPLY_INTEGER;
			}
		}
		else
		{
			assert(cur->type == REDIS_REPLY_ERROR || cur->type == REDIS_REPLY_STATUS);
			if (r->fn && r->fn->createString)
			{
				obj = r->fn->createString(cur, p, len);
			}
			else
			{
				obj = (void*)(size_t)(cur->type);
			}
		}

		if (obj == NULL)
		{
			r->err = REDIS_ERR_OOM;
			return REDIS_ERR;
		}

		// 如果当前解析的是根节点，设置以便返回
		if (r->ridx == 0)
			r->reply = obj;
		// 如果当前节点不是array类型，为什么需要moveToNextTask？
		// 因为当前节点要么是唯一的reply节点，要么是某个树的叶子结点，需要移动到右兄弟节点或者父节点
		moveToNextTask(r);
		return REDIS_OK;
	}
	return REDIS_ERR;
}


// 解析 REDIS_REPLY_ARRAY 类型的节点
// processAggregateItem
int processMultiBulkItem(redisReader* r)
{
	redisReadTask* cur = r->task[r->ridx];
	void* obj;
	char* p;
	long long elements;
	int root = 0, len;

//    if(r->ridx >= r->tasks-1)
//    {
//        LOG_DEBUG("r->ridx error: %d\n", r->ridx);
//        r->err=REDIS_ERR_PROTOCOL;
//        return REDIS_ERR;
//    }
    
    if(r->ridx > r->height)
    {
        r->height = r->ridx;
    }

	// 支持树的层数大于7
	if (r->ridx == r->tasks - 1)
	{
		if (redisReaderGrow(r) == REDIS_ERR)
			return REDIS_ERR;
	}

	if ((p = readLine(r, &len)) != NULL)
	{
		if (string2ll_norah(p, len, &elements) == REDIS_ERR)
		{
            LOG_DEBUG("string2ll_norah parse elements failed\n");
			r->err = REDIS_ERR_PROTOCOL;
			return REDIS_ERR;
		}

		root = (r->ridx == 0);

		//if (elements < -1 || (LLONG_MAX > SIZE_MAX  &&  elements > SIZE_MAX))
		if(elements<-1)
		{
            LOG_DEBUG("elements error: %lld\n", elements);
			r->err = REDIS_ERR_PROTOCOL;
			return REDIS_ERR;
		}

		if (elements == -1) // 空的 REDIS_REPLY_ARRAY 类型节点
		{
			if (r->fn && r->fn->createNil)
				obj = r->fn->createNil(cur);
			else
				obj = (void*)REDIS_REPLY_NIL;

			if (obj == NULL)
			{
				r->err = REDIS_ERR_OOM;
				return REDIS_ERR;
			}

			moveToNextTask(r);
		}
		else
		{
			if (r->fn && r->fn->createArray)
				obj = r->fn->createArray(cur, elements);
			else
				obj = (void*)(long)REDIS_REPLY_ARRAY;

			if (obj == NULL)
			{
				r->err = REDIS_ERR_OOM;
				return REDIS_ERR;
			}

			if (elements > 0) // dfs搜索
			{
				cur->elements = elements;
				cur->obj = obj;
				r->ridx++;
				r->task[r->ridx]->type = -1;
				r->task[r->ridx]->elements = -1;
				r->task[r->ridx]->idx = 0;
				r->task[r->ridx]->obj = NULL;
				r->task[r->ridx]->parent = cur;
				r->task[r->ridx]->privdata = r->privdata;
			}
			else // dfs回溯
			{
				moveToNextTask(r);
			}
		}

		// 设置根节点
		if (root)
			r->reply = obj;
		return REDIS_OK;
	}

	return REDIS_ERR;
}


void moveToNextTask(redisReader* r)
{
	redisReadTask* cur, * prv;
	// 循环用于向上回溯
	while (r->ridx >= 0)
	{
		// 当前解析的是第一层的根节点
		if (r->ridx == 0)
		{
			r->ridx--; // 标记解析完成
			return;
		}

		cur = r->task[r->ridx];
		prv = r->task[r->ridx - 1];
		assert(prv->type == REDIS_REPLY_ARRAY);
		if (cur->idx == prv->elements - 1)
		{
			r->ridx--;
		}
		else
		{
			assert(cur->idx < prv->elements);
			cur->type = -1;
			cur->elements = -1;
			cur->idx++;
			return;
		}
	}
}

// 解析 REDIS_REPLY_STRING 类型节点
int processBulkItem(redisReader* r)
{
	redisReadTask* cur = r->task[r->ridx];
	void* obj = NULL;
	char* p, * s;
	long long len;
	unsigned long bytelen;
	int success = 0;

    if(r->ridx > r->height)
    {   
        r->height = r->ridx;
    } 

	p = r->buf + r->pos;
	s = seekNewline(p, r->len - r->pos); // s指向 \r
	if (s != NULL)
	{
		p = r->buf + r->pos;
		bytelen = s - (r->buf + r->pos) + 2; // 包括\r\n的一行的总长度

		if (string2ll_norah(p, bytelen - 2, &len) == REDIS_ERR)
		{
			r->err = REDIS_ERR_PROTOCOL;
			return REDIS_ERR;
		}

		if (len < -1 || (LLONG_MAX > SIZE_MAX  && len > (long long)SIZE_MAX))
		{
			r->err = REDIS_ERR_PROTOCOL;
			return REDIS_ERR;
		}

		if (len == -1)
		{
			if (r->fn && r->fn->createNil)
				obj = r->fn->createNil(cur);
			else
				obj = (void*)REDIS_REPLY_NIL;
			success = 1;
		}
		else
		{
			bytelen += len + 2; // 包括字符串行的长度，加上字符串的长度
			if (r->pos + bytelen <= r->len) // 都读取到缓存中
			{
				if (r->fn && r->fn->createString)
					obj = r->fn->createString(cur, s + 2, len);
				else
					obj = (void*)REDIS_REPLY_STRING;
				success = 1;
			}
		}

		if (success)
		{
			if (obj == NULL)
			{
				r->err = REDIS_ERR_OOM;
				return REDIS_ERR;
			}
			r->pos += bytelen;

			// 设置根节点
			if (r->ridx == 0)
				r->reply = obj;
			moveToNextTask(r);
			return REDIS_OK;
		}
	}
	return REDIS_ERR;
}

// -------------------------------------------------------


// 每次解析一个节点
int processItem(redisReader* r)
{
	// 从最后解析的节点继续解析
	redisReadTask* cur = r->task[r->ridx];
	char* p;

	if (cur->type < 0)
	{
		if ((p = readBytes(r, 1)) != NULL)
		{
			switch (p[0])
			{
			case '-':
				cur->type = REDIS_REPLY_ERROR;
				break;
			case '+':
				cur->type = REDIS_REPLY_STATUS;
				break;
			case ':':
				cur->type = REDIS_REPLY_INTEGER;
				break;
			case '$':
				cur->type = REDIS_REPLY_STRING;
				break;
			case '*':
				cur->type = REDIS_REPLY_ARRAY;
				break;
			default:
				r->err = REDIS_ERR_PROTOCOL;
				LOG_DEBUG("error type %c\n", p[0]);
				return REDIS_ERR;
			}
		}
		else
		{
			return REDIS_ERR;
		}
	}

	LOG_DEBUG("cur->type is %s\n", desc(cur->type).c_str());
	switch (cur->type)
	{
	case REDIS_REPLY_ERROR:
	case REDIS_REPLY_STATUS:
	case REDIS_REPLY_INTEGER:
		return processLineItem(r);
	case REDIS_REPLY_STRING:
		return processBulkItem(r);
	case REDIS_REPLY_ARRAY:
		return processMultiBulkItem(r);
	default:
		assert(NULL);
		return REDIS_ERR;
	}
}



redisReader* redisReaderCreateWithFunctions(redisReplyObjectFunctions* fn)
{
	redisReader* r = (redisReader*)calloc(1, sizeof(redisReader));
	// 
	if (r == NULL)
		return NULL;

	r->buf = sdsempty();
	if (r->buf == NULL)
		goto oom;

	r->task = (redisReadTask**)calloc(REDIS_READER_STACK_SIZE, sizeof(*r->task));
	// redisReadTask** task;  r->task是redisReadTask**类型，*r->task是redisReadTask*类型
	// 分配 一个redisReadTask*类型大小的内存，返回的是 redisReadTask**指针类型
	// r->task = (redisReadTask**)calloc(REDIS_READER_STACK_SIZE, sizeof(redisReadTask*));
	if (r->task == NULL)
		goto oom;

	for (; r->tasks < REDIS_READER_STACK_SIZE; r->tasks++)
	{
		r->task[r->tasks] = (redisReadTask*)calloc(1, sizeof(**r->task));
		// **r->task是redisReadTask类型，
		// r->task[] 数组的每个成员时 redisReadTask*类型
		// r->task[r->tasks] = (redisReadTask*)calloc(1, sizeof(redisReadTask));
		if (r->task[r->tasks] == NULL)
			goto oom;
	}

	r->reply = NULL;
	r->fn = fn;
	r->maxbuf = REDIS_READER_MAX_BUF;
	r->ridx = -1;
    r->height = 0;

	return r;

oom:
	redisReaderFree(r);
	return NULL;
}

void redisReaderFree(redisReader* r)
{
	LOG_DEBUG("redisReaderFree\n");

	if (r == NULL)
		return;

	if (r->reply != NULL && r->fn && r->fn->freeObject)
	{
		//LOG_DEBUG("to free redis reply\n");
		r->fn->freeObject(r->reply);
	}

	for (int i = 0; i < r->tasks; i++)
		free(r->task[i]);

	if (r->task)
		free(r->task);

	sdsfree(r->buf);
	free(r);
}
