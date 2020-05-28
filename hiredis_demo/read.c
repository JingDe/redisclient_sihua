

/*

redisReply�� �����Ľṹ��ʾ�����õ�redis�ظ���Ϣ��һ��RedisReply��ʾ���е�һ���ڵ㡣


redisReader: ����redis�ظ��Ľ�����
	�洢redis�ظ���ԭʼ����
	����״̬����ǰ���������ĵڼ��㣬��ǰ�ڵ����丸�ڵ�ĵڼ������ӽڵ㡣
	�����������͵Ļظ��ڵ�redisReply�ķ�����


redisReadTask��
	���ڽ������е�ĳһ���ڵ㡣��¼��ǰ�ڵ�����ͣ����ӽڵ�ĸ������ڸ��ڵ�����е�������ָ�򸸽ڵ�redisReaderTask��ָ�롣

	redisReadTask�ڵ㣬�Զ�����Ľṹ��һһ��Ӧ���ս������� RedisReply���е�ÿһ���ڵ㡣

	ͨ��redisRedisTask�����ȸ�������˳�򣬹���redisReply����


�����Ľ������̷�����
	����һ���������������ܵĶ�ȡredis�Ļظ����ݡ�
	��ʼʱ���Ӹ��ڵ㿪ʼ������

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

// ��s��ʼ��Ѱ��\r\n������ָ��\r��ָ��
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
		r->pos += len + 2; // ָ����һ�еĿ�ʼλ��
		if (_len)
			*_len = len; // ���ص�ǰ�еĳ���
		return p; // �������׵�ַ
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

		// �����ǰ�������Ǹ��ڵ㣬�����Ա㷵��
		if (r->ridx == 0)
			r->reply = obj;
		// �����ǰ�ڵ㲻��array���ͣ�Ϊʲô��ҪmoveToNextTask��
		// ��Ϊ��ǰ�ڵ�Ҫô��Ψһ��reply�ڵ㣬Ҫô��ĳ������Ҷ�ӽ�㣬��Ҫ�ƶ������ֵܽڵ���߸��ڵ�
		moveToNextTask(r);
		return REDIS_OK;
	}
	return REDIS_ERR;
}


// ���� REDIS_REPLY_ARRAY ���͵Ľڵ�
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

	// ֧�����Ĳ�������7
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

		if (elements == -1) // �յ� REDIS_REPLY_ARRAY ���ͽڵ�
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

			if (elements > 0) // dfs����
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
			else // dfs����
			{
				moveToNextTask(r);
			}
		}

		// ���ø��ڵ�
		if (root)
			r->reply = obj;
		return REDIS_OK;
	}

	return REDIS_ERR;
}


void moveToNextTask(redisReader* r)
{
	redisReadTask* cur, * prv;
	// ѭ���������ϻ���
	while (r->ridx >= 0)
	{
		// ��ǰ�������ǵ�һ��ĸ��ڵ�
		if (r->ridx == 0)
		{
			r->ridx--; // ��ǽ������
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

// ���� REDIS_REPLY_STRING ���ͽڵ�
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
	s = seekNewline(p, r->len - r->pos); // sָ�� \r
	if (s != NULL)
	{
		p = r->buf + r->pos;
		bytelen = s - (r->buf + r->pos) + 2; // ����\r\n��һ�е��ܳ���

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
			bytelen += len + 2; // �����ַ����еĳ��ȣ������ַ����ĳ���
			if (r->pos + bytelen <= r->len) // ����ȡ��������
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

			// ���ø��ڵ�
			if (r->ridx == 0)
				r->reply = obj;
			moveToNextTask(r);
			return REDIS_OK;
		}
	}
	return REDIS_ERR;
}

// -------------------------------------------------------


// ÿ�ν���һ���ڵ�
int processItem(redisReader* r)
{
	// ���������Ľڵ��������
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
	// redisReadTask** task;  r->task��redisReadTask**���ͣ�*r->task��redisReadTask*����
	// ���� һ��redisReadTask*���ʹ�С���ڴ棬���ص��� redisReadTask**ָ������
	// r->task = (redisReadTask**)calloc(REDIS_READER_STACK_SIZE, sizeof(redisReadTask*));
	if (r->task == NULL)
		goto oom;

	for (; r->tasks < REDIS_READER_STACK_SIZE; r->tasks++)
	{
		r->task[r->tasks] = (redisReadTask*)calloc(1, sizeof(**r->task));
		// **r->task��redisReadTask���ͣ�
		// r->task[] �����ÿ����Աʱ redisReadTask*����
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
