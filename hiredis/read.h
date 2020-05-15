

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

enum RedisReplyType {
	REDIS_REPLY_ERROR = 1,
	REDIS_REPLY_STATUS,
	REDIS_REPLY_NIL, // ����Ϊ-1�Ľڵ�
	REDIS_REPLY_INTEGER,
	REDIS_REPLY_STRING=1,
	REDIS_REPLY_ARRAY=2,	
};

typedef struct redisReply {
	int type; // �ظ�����
	long long integer;
	double dval;
	size_t len;
	char* str;

	size_t elements;
	struct redisReply** element;
};

// ���ȳ�����ʽ����
//enum RedisErrType {
//	kRedisErrOk=0,
//	kRedisErrProtocol=-1,
//	kRedisErrOOM=-2,
//};

// �귽ʽҲ��
enum RedisErrType {
	REDIS_ERR_OK=0,
	REDIS_ERR_PROTOCOL=-1,
	REDIS_ERR_OOM=-2,
};

enum RedisState {
	REDIS_OK=0,
	REDIS_ERR=-1,
};

typedef struct redisReader {
	int err;
	char errstr[128];
	
	vector<char> buf;

	//char* buf; // �洢reply�����뻺��
	//size_t pos; // ��ǰ����λ��
	//size_t len; 
	// size_t maxbuf; // �������е������д�С������ʱɾ���ѽ�����������

	redisReadTask** task;
	int tasks; // task�ĳ���
	int ridx; // ��ǰ�������ڼ���
	void* reply; // �洢���ս�������ĸ��ڵ�?

	redisReplyObjectFunctions* fn; // �����������͵ķ���
	void* privdata;
} redisReader;


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
	
	void* (*freeObject)(void*);
};

// ���ԣ����ļ��ж�ȡreply
typedef struct redisContext {
	std::string replyFile;
	int fd;

	redisReader* reader;

	int err;

	void* (*read)(char*, size_t);
};


int redisGetReply(redisContext* c, RedisReply** reply)
{
	// �����ѷ�������

	RedisReply* aux = NULL;

	do
	{
		// ��redis�����ж�ȡ�ظ���ÿ������ȡһ��̶���С�����ݣ������� redisReader��buffer��
		if (redisBufferRead(c) == REDIS_ERR)
			return REDIS_ERR;

		// ����redisReader������������
		//if (redisGetReplyFromReader(c, &aux) == REDIS_ERR)
		if (redisReaderGetReply(c->reader, &aux) == REDIS_ERR)
			return REDIS_ERR;
	} while (aux == NULL);

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
	if (nread > 0)
	{
		// ����ȡ�����ݣ�׷�ӵ�redisReader�����뻺����������
		if (redisReaderFeed(c->reader, buf, nread) != REDIS_OK)
		{
			return REDIS_ERR;
		}
		else
		{
		}
	}
	else if (nread < 0)
	{
		return REDIS_ERR;
	}
	return REDIS_OK;
}

// ��̬���� redisReader�����뻺��
int redisReaderFeed(redisReader* r, const char* buf, size_t len)
{
	if (r->err)
		return REDIS_ERR;

	if (buf == NULL || len < 1)
		return REDIS_ERR;

	/*r->buf.resize(len);
	for (int i = 0; i < len; i++)
	{
		r->buf[i] = *(buf + i);
	}*/


	// TODO
	// return REDIS_ERR_OOOM ??

	return REDIS_OK;
}


//int redisGetReplyFromReader(redisContext* c, void** reply)
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

	if (r->buf.empty())
		return REDIS_OK;

	if (r->ridx == -1)
	{
		// ��ʼ����һ�������ڵ㣬��һ��ĸ��ڵ�
		r->task[0]->type = -1;
		r->task[0]->elements = -1;
		r->task[0]->idx = -1;
		r->task[0]->obj = NULL;
		r->task[0]->parent = NULL;
		r->task[0]->privdata = r->privdat;

		// ��ʼ�ӵ�һ�㿪ʼ����
		r->ridx = 0;
	}

	// ѭ����ÿ�ν���һ���ڵ㣬dfs˳��
	while (r->ridx >= 0)
	{
		if (processItem(r) != REDIS_OK)
			break;
	}

	if (r->err)
		return REDIS_ERR;

	/*if (r->pos >= 1024)
	{

	}*/

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

// ÿ�ν���һ���ڵ�
static int processItem(redisReader* r)
{
	// ���������Ľڵ��������
	redisReadTask* cur = r->task[r->ridx];
	char* p;

	if (cur->type < 0)
	{
		if (p = readBytes(r, 1) != NULL)
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
				return REDIS_ERR;
			}
		}
		else
		{
			return REDIS_ERR;
		}
	}

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

static int processLineItem(redisReader* r)
{
	redisReadTask* cur = r->task[r->ridx];
	void* obj;
	char* p;
	int len;

	if ((p = readLine(r, &len)) != NULL)
	{
		if (cur->type == REDIS_REPLY_INTEGER)
		{
			if (r->fn && r->fn->createInteger)
			{
				long long v;
				if (string2ll(p, len, &v) == REDIS_ERR)
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

static void moveToNextTask(redisReader* r)
{
	redisReadTask* cur, * prev;
	// ѭ���������ϻ���
	while (r->idx >= 0)
	{
		// ��ǰ�������ǵ�һ��ĸ��ڵ�
		if (r->ridx == 0)
		{
			r->ridx--; // ��ǽ������
			return;
		}

		cur = r->task[r->idx];
		prv = r->task[r->idx - 1];
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

// processAggregateItem
static int processBulkItem(redisReader* r)
{
	redisReadTask* cur = r->task[r->idx];
	void* obj;
	char* p;
	long long elements;
	int root = 0, len;

	// ֧�����Ĳ�������7
	if (r->ridx == r->tasks - 1)
	{
		if (redisReaderGrow(r) == REDIS_ERR)
			return REDIS_ERR;
	}

	if ((p = readLine(r, &len)) != NULL)
	{
		if (string2ll(p, len, &elements) == REDIS_ERR)
		{
			r->err = REDIS_ERR_PROTOCOL;
			return REDIS_ERR;
		}

		root = (r->ridx == 0);

		if (elements < -1 || (LLONG_MAX > SIZE_MAX  &&  elements > SIZE_MAX))
		{
			r->err = REDIS_ERR_PTOTOCOL;
			return REDIS_ERR;
		}

		if (elements == -1)
		{
			if (r->fn && r->fn->createNil)
				obj = f->fn->createNil(cur);
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
				moveToNexTask(r);
			}
		}

		if (root)
			r->reply = obj;
		return REDIS_OK;
	}

	return REDIS_ERR;
}

static redisReply* createReplyObject(int type)
{
	redisReply* r = calloc(1, sizeof(*r));

	if (r == NULL)
		return NULL;

	r->type = type;
	return r;
}

static redisReplyObjectFunction defaultFunctions = {
	createStringObject,
	createArrayObject,
	createIntegerObject,
	createNilObject,

	freeReplyObject
};

static void* createIntegerObject(const redisReadTask* task, long long value)
{
	redisReply* r, * parent;

	r = createReplyObject(REDIS_REPLY_INTEGER);
	if (r == NULL)
		return NULL;

	r->integer = value;
	if (task->parent)
	{
		parent = task->parent->obj;
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

	r->integer = value;
	if (task->parent)
	{
		parent = task->parent->obj;
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}
	return r;
}

static void* createStringObject(const redisReadTask* task, char* str, int len)
{
	redisReply* r, * parent;
	char* buf;

	r->createReplyObject(task->type);
	if (r->NULL)
		return NULL;

	buf = malloc(len + 1);
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
		parent = task->parent->obj;
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}
	return r;
}

static void* createArrayObject(const redisReadTask* task, size_t elements)
{
	r = createReplyObject(task->type);
	if (r == NULL)
		return NULL;

	if (elements > 0)
	{
		r->element = calloc(elements, sizeof(redisReply*));
		if (r->element == NULL)
		{
			freeReplyObject(r);
			return NULL;
		}
	}

	r->elements = elements;

	if (task->parent)
	{
		parent = task->parent->obj;
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}
	return r;
}

void freeReplyObject(void* reply)
{
	redisReply* r = reply;
	size_t j;

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

// ���� REDIS_REPLY_STRING ���ͽڵ�
static int processBulkItem(redisReader* r)
{
	redisReadTask* cur = r->task[r->ridx];
	void* obj = NULL;
	char* p, * s;

}