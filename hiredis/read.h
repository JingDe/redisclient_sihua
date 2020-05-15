

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

enum RedisReplyType {
	REDIS_REPLY_ERROR = 1,
	REDIS_REPLY_STATUS,
	REDIS_REPLY_NIL, // 长度为-1的节点
	REDIS_REPLY_INTEGER,
	REDIS_REPLY_STRING=1,
	REDIS_REPLY_ARRAY=2,	
};

typedef struct redisReply {
	int type; // 回复类型
	long long integer;
	double dval;
	size_t len;
	char* str;

	size_t elements;
	struct redisReply** element;
};

// 优先常量方式命名
//enum RedisErrType {
//	kRedisErrOk=0,
//	kRedisErrProtocol=-1,
//	kRedisErrOOM=-2,
//};

// 宏方式也可
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

	//char* buf; // 存储reply的输入缓存
	//size_t pos; // 当前解析位置
	//size_t len; 
	// size_t maxbuf; // 允许缓存中的最大空闲大小，超出时删除已解析部分数据

	redisReadTask** task;
	int tasks; // task的长度
	int ridx; // 当前解析到第几层
	void* reply; // 存储最终解析结果的根节点?

	redisReplyObjectFunctions* fn; // 解析基本类型的方法
	void* privdata;
} redisReader;


typedef struct redisReadTask {
	int type; // 当前解析节点对应回复节点的类型
	int elements; // 子节点个数
	int idx; // 当前节点在父节点儿子节点中的索引
	void* obj; // 对应的回复节点
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

// 测试，从文件中读取reply
typedef struct redisContext {
	std::string replyFile;
	int fd;

	redisReader* reader;

	int err;

	void* (*read)(char*, size_t);
};


int redisGetReply(redisContext* c, RedisReply** reply)
{
	// 假设已发送命令

	RedisReply* aux = NULL;

	do
	{
		// 从redis连接中读取回复，每次最多读取一块固定大小的数据，拷贝到 redisReader的buffer中
		if (redisBufferRead(c) == REDIS_ERR)
			return REDIS_ERR;

		// 调用redisReader继续解析数据
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

	// 从套接字中读取数据
	nread = c->read(buf, sizeof(buf));
	if (nread > 0)
	{
		// 将读取的数据，追加到redisReader的输入缓存中来解析
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

// 动态扩大 redisReader的输入缓存
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
		// 初始化第一个解析节点，第一层的根节点
		r->task[0]->type = -1;
		r->task[0]->elements = -1;
		r->task[0]->idx = -1;
		r->task[0]->obj = NULL;
		r->task[0]->parent = NULL;
		r->task[0]->privdata = r->privdat;

		// 开始从第一层开始解析
		r->ridx = 0;
	}

	// 循环，每次解析一个节点，dfs顺序
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

	// 解析到到完整的reply
	if (r->ridx == -1)
	{
		if (reply != NULL)
		{
			// 将触发调用方解析完成流程
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

// 每次解析一个节点
static int processItem(redisReader* r)
{
	// 从最后解析的节点继续解析
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

static void moveToNextTask(redisReader* r)
{
	redisReadTask* cur, * prev;
	// 循环用于向上回溯
	while (r->idx >= 0)
	{
		// 当前解析的是第一层的根节点
		if (r->ridx == 0)
		{
			r->ridx--; // 标记解析完成
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

	// 支持树的层数大于7
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

// 解析 REDIS_REPLY_STRING 类型节点
static int processBulkItem(redisReader* r)
{
	redisReadTask* cur = r->task[r->ridx];
	void* obj = NULL;
	char* p, * s;

}