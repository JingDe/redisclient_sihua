#include"hiredis.h"
#include"util.h"
#include"read.h"
#include"sds.h"

#include<cassert>

#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<cstring>


static redisReply* createReplyObject(int type)
{
	redisReply* r = (redisReply*)calloc(1, sizeof(*r));

	if (r == NULL)
		return NULL;
	
//	LOG_DEBUG("DEBUG createReplyObject type: %d pointer: %p\n", type, r);

	r->type = type;
	return r;
}


void freeReplyObject(void* reply)
{
	//LOG_DEBUG("freeReplyObject %p\n", reply);

	redisReply* r = static_cast<redisReply*>(reply);
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

static void* createIntegerObject(const redisReadTask* task, long long value)
{
	redisReply* r, * parent;

	r = createReplyObject(REDIS_REPLY_INTEGER);
	if (r == NULL)
		return NULL;

	r->integer = value;
	if (task->parent)
	{
		parent = static_cast<redisReply*>(task->parent->obj);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}
	return r;
}

static void* createNilObject(const redisReadTask* task)
{
	redisReply* r, * parent;

	r = createReplyObject(REDIS_REPLY_NIL);
	if (r == NULL)
		return NULL;

	if (task->parent)
	{
		parent = static_cast<redisReply*>(task->parent->obj);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}
	return r;
}

static void* createStringObject(const redisReadTask* task, char* str, size_t len)
{
	redisReply* r, * parent;
	char* buf;

	r=createReplyObject(task->type);
	if (r==NULL)
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
		parent = static_cast<redisReply*>(task->parent->obj);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}
	return r;
}

static void* createArrayObject(const redisReadTask* task, size_t elements)
{
	redisReply* r, * parent;

	r = createReplyObject(task->type);
	if (r == NULL)
		return NULL;

	if (elements > 0)
	{
		r->element = reinterpret_cast<redisReply**>(calloc(elements, sizeof(redisReply*)));
		if (r->element == NULL)
		{
			freeReplyObject(r);
			return NULL;
		}
	}

	r->elements = elements;

	if (task->parent)
	{
		parent = static_cast<redisReply*>(task->parent->obj);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}
	return r;
}



static redisReplyObjectFunctions defaultFunctions = {
	createStringObject,
	createArrayObject,
	createIntegerObject,
	createNilObject,

	freeReplyObject
};

// ---------------------------------------------------

int redisGetReply(redisContext* c, void** reply)
{
	// �����ѷ�������

	void* aux = NULL;

	do
	{
		// ��redis�����ж�ȡ�ظ���ÿ������ȡһ��̶���С�����ݣ������� redisReader��buffer��
		if (redisBufferRead(c) == REDIS_ERR)
	    {
            LOG_DEBUG("read reply failed: %d, %d\n", c->reader->err, c->err);
    		return REDIS_ERR;
        }

		// ����redisReader������������
		//if (redisGetReplyFromReader(c, &aux) == REDIS_ERR)
		if (redisReaderGetReply(c->reader, &aux) == REDIS_ERR)
		{
            LOG_DEBUG("parse err: %d, %d\n", c->reader->err, c->err);
            return REDIS_ERR;
        }
	} while (aux == NULL);

    if(aux != NULL)
    {
        LOG_DEBUG("reply tree level: %d\n", c->reader->height);
    }

	if (reply != NULL)
	{
		*reply = aux;
	}
	else
	{
		freeReplyObject(aux);
	}
	return REDIS_OK;
}

int redisBufferRead(redisContext* c)
{
	char buf[1024 * 16];
	int nread;

	if (c->err)
		return REDIS_ERR;

	// ���׽����ж�ȡ����
	nread = c->read(buf, sizeof(buf));
	LOG_DEBUG("read return %d bytes\n", nread);
	if (nread > 0)
	{
		buf[nread] = 0;
		LOG_DEBUG("read data: [%s]\n", buf);

		// ����ȡ�����ݣ�׷�ӵ�redisReader�����뻺����������
		if (redisReaderFeed(c->reader, buf, nread) != REDIS_OK)
		{
			return REDIS_ERR;
		}
		else
		{
		}
	}
	else if (nread <= 0)
	{
		return REDIS_ERR;
	}
	return REDIS_OK;
}

// TODO
// ����bufָ���len���ȵ����ݵ�r�����뻺������
// 
int redisReaderFeed(redisReader* r, const char* buf, size_t len)
{
	sds newbuf;

	if (r->err)
		return REDIS_ERR;

	if (buf != NULL && len >= 1)
	{
		// ��ǰ�����뻺���ǿյģ����ҳ���maxbuf�����ͷ�����������
		if (r->len == 0 && r->maxbuf != 0 && sdsavail(r->buf) > r->maxbuf)
		{
			sdsfree(r->buf);
			r->buf = sdsempty();
			r->pos = 0;

			assert(r->buf != NULL);
		}

		newbuf = sdscatlen(r->buf, buf, len);
		if (newbuf == NULL)
		{
			r->err = REDIS_ERR_OOM;
			return REDIS_ERR;
		}

		r->buf = newbuf;
		r->len = sdslen(r->buf);
	}

	return REDIS_OK;
}


//int redisGetReplyFromReader(redisContext* c, redisReply** reply)
//{
//	if (redisReaderGetReply(c->reader, reply) == REDIS_ERR)
//		return REDIS_ERR;
//
//	return REDIS_OK;
//}

int redisReaderGetReply(redisReader* r, void** reply)
{
	if (reply != NULL)
		*reply = NULL;

	if (r->err)
		return REDIS_ERR;

	if (r->len==0)
		return REDIS_OK;

	if (r->ridx == -1)
	{
		// ��ʼ����һ�������ڵ㣬��һ��ĸ��ڵ�
		r->task[0]->type = -1;
		r->task[0]->elements = -1;
		r->task[0]->idx = -1;
		r->task[0]->obj = NULL;
		r->task[0]->parent = NULL;
		r->task[0]->privdata = r->privdata;

		// ��ʼ�ӵ�һ�㿪ʼ����
		r->ridx = 0;
	}

	// ѭ����ֱ�����������еĽڵ㣬���߽����굱ǰ�����������а��������нڵ㣬���߽�������
	while (r->ridx >= 0) // ��ʾδ������ȫ
	{
		// ÿ�ν���һ���ڵ㣬dfs˳��
		if (processItem(r) != REDIS_OK)
			break;
	}

	if (r->err)
		return REDIS_ERR;

	if (r->pos >= 1024)
	{
		sdsrange(r->buf, r->pos, -1);
		r->pos = 0;
		r->len = sdslen(r->buf);
	}

	// ��������������reply
	if (r->ridx == -1)
	{
		if (reply != NULL)
		{
			// ���������÷������������
			*reply = r->reply;
		}
		else if (r->reply != NULL && r->fn && r->fn->freeObject)
		{
			r->fn->freeObject(r->reply);
		}
		r->reply = NULL;
	}

	return REDIS_OK;
}

// test
ssize_t redisContext::read(char* buf, size_t len)
{
	if (err || fd < 0 || buf == NULL)
		return -1;
	ssize_t n = ::read(fd, buf, len);

	return n;
}

bool redisContextInit(const std::string& filename, redisContext* rc)
{
	if (rc == NULL)
		return false;

	rc->err = REDIS_ERR_OK;
	rc->reader = NULL;
	rc->fd = -1;

	//rc->reader = redisReaderCreate();
	rc->reader = redisReaderCreateWithFunctions(&defaultFunctions);
	if (rc->reader == NULL)
	{
		rc->err = REDIS_ERR_OOM;
		return false;
	}

	rc->replyfile = filename;
	rc->fd = ::open(filename.c_str(), O_RDONLY);
	if (rc->fd < 0)
	{
		rc->err = REDIS_ERR_DEBUG;
		return false;
	}

	return true;
}

bool redisContextUninit(redisContext* rc)
{
	if (rc == NULL)
		return true;

	redisReaderFree(rc->reader);

	if (rc->fd > 0)
	{
		::close(rc->fd);
	}
	return true;
}
