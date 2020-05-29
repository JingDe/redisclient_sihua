#include "redisconnection.h"
#include "redisclient.h"
#include "base_library/log.h"
#include "sds.h"
#include "util.h"

#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

#include <cassert>
#include <sstream>
#include <climits>

namespace GBDownLinker {


RedisReplyObjectFunctions RedisConnection::defaultFunctions = {
    createStringObject,
    createArrayObject,
    createIntegerObject,
    createNilObject,

    freeReplyObject
};

static std::string desc(int type)
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

RedisConnection::RedisConnection(const string serverIp, uint32_t serverPort, uint32_t connectTimeout, uint32_t readTimeout, const std::string& passwd)
{
    m_serverIp = serverIp;
    m_serverPort = serverPort;
    m_connectTimeout = connectTimeout;
    m_readTimeout = readTimeout;
    m_passwd = passwd;
    m_unparseBuf = NULL;
    m_unparseLen = 0;
    m_parseState = REDIS_PARSE_UNKNOWN_STATE;
    m_valid = false;
    m_arrayNum = 0;
    m_doneNum = 0;
    m_arrayLen = 0;
    m_canRelease = true;

    m_reader = new RedisReader();
    if (m_reader == NULL)
    {
        LOG_WRITE_ERROR("create RedisReader failed");
    }
}

RedisConnection::~RedisConnection()
{
    m_socket.close();
}

void RedisConnection::SetCanRelease(bool canRelease)
{
    m_canRelease = canRelease;
}

bool RedisConnection::connect()
{
    if (!m_socket.connect(m_serverIp, m_serverPort, m_connectTimeout))
    {
        std::stringstream log_msg;
        log_msg << "connection connect to server:[" << m_serverIp << ":" << m_serverPort << "] failed.";
        LOG_WRITE_ERROR(log_msg.str());
        return false;
    }

    if(!m_passwd.empty()  &&  !AuthPasswd())
    {
        LOG_WRITE_ERROR("auth passwd failed, connect failed");
        return false;
    }
    return true;
}

bool RedisConnection::close()
{
    m_socket.close();
    return true;
}

bool RedisConnection::freeReader()
{
    if (m_reader == NULL)
        return false;

    if (m_reader->reply != NULL)
    {
        defaultFunctions.freeObject(m_reader->reply);
    }

    for (int i = 0; i < m_reader->tasks; i++)
        free(m_reader->task[i]);

    if (m_reader->task)
        free(m_reader->task);

    sdsfree(m_reader->buf);
    return true;
}

bool RedisConnection::initReader()
{
    if (m_reader == NULL)
        return false;

    m_reader->buf = sdsempty();
    if (m_reader->buf == NULL)
        goto oom;

	m_reader->pos=0;
	m_reader->len=0;

    m_reader->task = (RedisReadTask**)calloc(REDIS_READER_STACK_SIZE, sizeof(*m_reader->task));
    // RedisReadTask** task;  r->task是RedisReadTask**类型，*r->task是RedisReadTask*类型
    // 分配 一个RedisReadTask*类型大小的内存，返回的是 RedisReadTask**指针类型
    // r->task = (RedisReadTask**)calloc(REDIS_READER_STACK_SIZE, sizeof(RedisReadTask*));
    if (m_reader->task == NULL)
        goto oom;

    for (m_reader->tasks=0; m_reader->tasks < REDIS_READER_STACK_SIZE; m_reader->tasks++)
    {
        m_reader->task[m_reader->tasks] = (RedisReadTask*)calloc(1, sizeof(**m_reader->task));
        // **r->task是RedisReadTask类型，
        // r->task[] 数组的每个成员时 RedisReadTask*类型
        // r->task[r->tasks] = (RedisReadTask*)calloc(1, sizeof(RedisReadTask));
        if (m_reader->task[m_reader->tasks] == NULL)
            goto oom;
    }

    m_reader->reply = NULL;
    m_reader->maxbuf = REDIS_READER_MAX_BUF;
    m_reader->ridx = -1;
    m_reader->height = 0;

    return true;

oom:
    freeReader();
    return false;
}

bool RedisConnection::resetReader()
{
	// free
    freeReader();
	// re-init
    return initReader();
}

bool RedisConnection::doRedisCommand(list < RedisCmdParaInfo >& paraList, int32_t paraLen, RedisReplyInfo& replyInfo, ReplyParserType parserType)
{
    checkConnectionStatus();
    if (m_socket.fd == INVALID_SOCKET_HANDLE)
    {
        std::stringstream log_msg;
        log_msg << "connection:[" << this << "] socket may be closed by peer.";
        LOG_WRITE_WARNING(log_msg.str());

        if (!connect())
        {
            return false;
        }
    }
    char* commandBuf = NULL;
    commandBuf = (char*)malloc(paraLen);
    memset(commandBuf, 0, paraLen);
    int32_t cmdLen = 0;
    createRedisCommand(paraList, &commandBuf, cmdLen);

    if (!send(commandBuf, cmdLen))
    {
        std::stringstream log_msg;
        log_msg << "connection:[" << this << "] send command:[" << commandBuf << "] to redis:" << m_serverIp << ":" << m_serverPort << " failed.";
        LOG_WRITE_ERROR(log_msg.str());
        free(commandBuf);
        commandBuf = NULL;
        return false;
    }
    free(commandBuf);
    commandBuf = NULL;

    return recv(replyInfo, parserType);
}

bool RedisConnection::doRedisCommand(list < RedisCmdParaInfo >& paraList, int32_t paraLen, RedisReply** replyInfo)
{
    if (m_reader == NULL || !resetReader())
    {
        LOG_WRITE_ERROR("setup RedisReader failed");
        return false;
    }

    checkConnectionStatus();
    if (m_socket.fd == INVALID_SOCKET_HANDLE)
    {
        std::stringstream log_msg;
        log_msg << "connection:[" << this << "] socket may be closed by peer.";
        LOG_WRITE_WARNING(log_msg.str());

        if (!connect())
        {
            return false;
        }
    }
    char* commandBuf = NULL;
    commandBuf = (char*)malloc(paraLen);
    memset(commandBuf, 0, paraLen);
    int32_t cmdLen = 0;
    createRedisCommand(paraList, &commandBuf, cmdLen);

    if (!send(commandBuf, cmdLen))
    {
        std::stringstream log_msg;
        log_msg << "connection:[" << this << "] send command:[" << commandBuf << "] to redis:" << m_serverIp << ":" << m_serverPort << " failed.";
        LOG_WRITE_ERROR(log_msg.str());
        free(commandBuf);
        commandBuf = NULL;
        return false;
    }
    free(commandBuf);
    commandBuf = NULL;

    bool res = GetReply(replyInfo);
    return res;
}

bool RedisConnection::send(char* request, uint32_t sendLen)
{
    return m_socket.writeFull((void*)request, sendLen);
}

ConnectionStatus RedisConnection::CheckConnected()
{
    if(m_socket.fd!=INVALID_SOCKET_HANDLE)
    {
        return ConnectionStatus::Connected;
    }

    LOG_WRITE_WARNING("try to reconnect");
    if(connect())
    {
        return ConnectionStatus::Reconnected;
    }
    else
    {
        return ConnectionStatus::Disconnected;
    }
}

bool RedisConnection::GetReply(RedisReply** reply)
{
    std::stringstream log_msg;
	
	if(reply!=NULL)
		*reply=NULL;
    RedisReply* aux = NULL; // 获取reply的根节点指针

    do
    {
        // 从redis连接中读取回复，每次最多读取一块固定大小的数据，拷贝到 RedisReader的buffer中
        if (RedisBufferRead() == REDIS_ERR)
        {
            log_msg<<"read reply failed: "<<m_reader->err;
            LOG_WRITE_ERROR(log_msg.str());
            return false;
        }

        // 调用RedisReader继续解析数据，直到解析成功或者失败
        if (RedisReaderGetReply(m_reader, &aux) == REDIS_ERR)
        {
            log_msg.str("");
            log_msg<<"parse err: "<<m_reader->err;
            return false;
        }
    } while (aux == NULL);

    if (aux != NULL)
    {
        log_msg.str("");
        log_msg<<"get reply ok, reply tree level: "<<m_reader->height;
        LOG_WRITE_INFO(log_msg.str());

        *reply = aux;
    }
    return true;
}

int RedisConnection::RedisBufferRead()
{
    std::stringstream log_msg;

    char buf[1024 * 16];
    int nread;

    // 从套接字中读取数据
    nread = ReadReply(buf, sizeof(buf)-1);
    log_msg<<"read return "<<nread<<"bytes";
    LOG_WRITE_INFO(log_msg.str());

    if (nread > 0)
    {
        buf[nread] = 0;

        log_msg.str("");
        log_msg<<"read data: "<<buf;
        LOG_WRITE_INFO(log_msg.str());

        // 将读取的数据，追加到RedisReader的输入缓存中来解析
        if (RedisReaderFeed(m_reader, static_cast<char*>(buf), nread) != REDIS_OK)
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
    else// if (nread == 0)
    {
        return REDIS_ERR; // TODO
    }
    return REDIS_OK;
}

int RedisConnection::ReadReply(char* buf, std::size_t len)
{
    int recvLen = m_socket.read(buf, len, m_readTimeout);
    return recvLen;
}

int RedisConnection::RedisReaderGetReply(RedisReader* r, RedisReply** reply)
{
    if (reply != NULL)
        *reply = NULL;

    if (r->err)
        return REDIS_ERR;

    if (r->len == 0)
        return REDIS_OK;

    if (r->ridx == -1)
    {
        // 初始化第一个解析节点，第一层的根节点
        r->task[0]->type = -1;
        r->task[0]->elements = -1;
        r->task[0]->idx = -1;
        r->task[0]->obj = NULL;
        r->task[0]->parent = NULL;
        r->task[0]->privdata = r->privdata;

        // 开始从第一层开始解析
        r->ridx = 0;
    }

    // 循环，直到解析完所有的节点，或者解析完当前读到的数据中包含的所有节点，或者解析出错
    while (r->ridx >= 0) // 表示未解析完全
    {
        // 每次解析一个节点，dfs顺序
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

    // 解析到到完整的reply
    if (r->ridx == -1)
    {
        if (reply != NULL)
        {
            // 将触发调用方解析完成流程
            *reply = r->reply;
        }
        else if (r->reply != NULL) // 调用方不需要reply结果
        {
            defaultFunctions.freeObject(r->reply);
        }
        r->reply = NULL;
    }

    return REDIS_OK;
}

// 每次解析一个节点
int RedisConnection::processItem(RedisReader* r)
{
    std::stringstream log_msg;

    // 从最后解析的节点继续解析
    RedisReadTask* cur = r->task[r->ridx];
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
                    log_msg.str("");
                    log_msg<<"error type "<<p[0];
                    LOG_WRITE_INFO(log_msg.str());
                    return REDIS_ERR;
            }
        }
        else
        {
            return REDIS_ERR;
        }
    }

    log_msg.str("");
    log_msg<<"cur->type is "<<desc(cur->type);
    LOG_WRITE_INFO(log_msg.str());

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

char* RedisConnection::readBytes(RedisReader* r, unsigned int bytes)
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

char* RedisConnection::readLine(RedisReader* r, int* _len)
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


// 从s开始，寻找\r\n，返回指向\r的指针
char* RedisConnection::seekNewline(char* s, size_t len)
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

int RedisConnection::processLineItem(RedisReader* r)
{
    RedisReadTask* cur = r->task[r->ridx];
    RedisReply* obj;
    char* p;
    int len;

    if (r->ridx > r->height)
    {
        r->height = r->ridx;
    }

    if ((p = readLine(r, &len)) != NULL)
    {
        if (cur->type == REDIS_REPLY_INTEGER)
        {
            long long v;
            if (string2ll_norah(p, len, &v) == REDIS_ERR)
            {
                r->err = REDIS_ERR_PROTOCOL;
                return REDIS_ERR;
            }
            obj = defaultFunctions.createInteger(cur, v);
        }
        else
        {
            assert(cur->type == REDIS_REPLY_ERROR || cur->type == REDIS_REPLY_STATUS);
            obj = defaultFunctions.createString(cur, p, len);
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


// 解析 REDIS_REPLY_STRING 类型节点
int RedisConnection::processBulkItem(RedisReader* r)
{
    RedisReadTask* cur = r->task[r->ridx];
    RedisReply* obj = NULL;
    char* p, * s;
    long long len;
    unsigned long bytelen;
    int success = 0;

    if (r->ridx > r->height)
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

        if (len < -1 || (LLONG_MAX > SIZE_MAX&& len > (long long)SIZE_MAX))
        {
            r->err = REDIS_ERR_PROTOCOL;
            return REDIS_ERR;
        }

        if (len == -1)
        {
            obj = defaultFunctions.createNil(cur);
            success = 1;
        }
        else
        {
            bytelen += len + 2; // 包括字符串行的长度，加上字符串的长度
            if (r->pos + bytelen <= r->len) // 都读取到缓存中
            {
                obj = defaultFunctions.createString(cur, s + 2, len);
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


// 解析 REDIS_REPLY_ARRAY 类型的节点
// processAggregateItem
int RedisConnection::processMultiBulkItem(RedisReader* r)
{
    std::stringstream log_msg;

    RedisReadTask* cur = r->task[r->ridx];
    RedisReply* obj;
    char* p;
    long long elements;
    int root = 0, len;

    //    if(r->ridx >= r->tasks-1)
    //    {
    //        LOG_DEBUG("r->ridx error: %d\n", r->ridx);
    //        r->err=REDIS_ERR_PROTOCOL;
    //        return REDIS_ERR;
    //    }

    if (r->ridx > r->height)
    {
        r->height = r->ridx;
    }

    // 支持树的层数大于7
    if (r->ridx == r->tasks - 1)
    {
        if (RedisReaderGrow(r) == REDIS_ERR)
            return REDIS_ERR;
    }

    if ((p = readLine(r, &len)) != NULL)
    {
        if (string2ll_norah(p, len, &elements) == REDIS_ERR)
        {
            log_msg.str("");
            log_msg<<"string2ll_norah parse elements failed";
            LOG_WRITE_INFO(log_msg.str());

            r->err = REDIS_ERR_PROTOCOL;
            return REDIS_ERR;
        }

        root = (r->ridx == 0);

        //if (elements < -1 || (LLONG_MAX > SIZE_MAX  &&  elements > SIZE_MAX))
        if (elements < -1)
        {
            log_msg.str("");
            log_msg<<"elements error: "<< elements;
            LOG_WRITE_INFO(log_msg.str());

            r->err = REDIS_ERR_PROTOCOL;
            return REDIS_ERR;
        }

        if (elements == -1) // 空的 REDIS_REPLY_ARRAY 类型节点
        {
            obj = defaultFunctions.createNil(cur);

            if (obj == NULL)
            {
                r->err = REDIS_ERR_OOM;
                return REDIS_ERR;
            }

            moveToNextTask(r);
        }
        else
        {
            obj = defaultFunctions.createArray(cur, elements);

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

int RedisConnection::RedisReaderGrow(RedisReader* r)
{
    RedisReadTask** aux;
    int newlen;

    newlen = r->tasks + REDIS_READER_STACK_SIZE;
    aux = static_cast<RedisReadTask**>(realloc(r->task, sizeof(*r->task) * newlen));
    if (aux == NULL)
        goto oom;

    r->task = aux;

    for (; r->tasks < newlen; r->tasks++)
    {
        r->task[r->tasks] = static_cast<RedisReadTask*>(calloc(1, sizeof(**r->task)));
        if (r->task[r->tasks] == NULL)
            goto oom;
    }

    return REDIS_OK;

oom:
    r->err = REDIS_ERR_PROTOCOL;
    return REDIS_ERR;
}

int RedisConnection::RedisReaderFeed(RedisReader* r, const char* buf, size_t len)
{
    sds newbuf;

    if (r->err)
        return REDIS_ERR;

    if (buf != NULL && len >= 1)
    {
        // 当前的输入缓存是空的，并且超过maxbuf，则释放整个缓冲区
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

void RedisConnection::moveToNextTask(RedisReader* r)
{
	RedisReadTask* cur, * prv;
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


bool RedisConnection::recv(RedisReplyInfo& replyInfo, ReplyParserType parserType)
{
	//init parse data
	m_unparseBuf = NULL;
	m_unparseLen = 0;
	m_parseState = REDIS_PARSE_UNKNOWN_STATE;
	m_valid = false;
	m_arrayNum = 0;
	m_doneNum = 0;
	m_arrayLen = 0;
	//recv data.
	char recvBuf[REDIS_READ_BUFF_SIZE];
	char* toParseBuf = NULL;
	int32_t mallocLen = 0;
	int32_t toParseLen = 0;
	toParseBuf = (char*)malloc(REDIS_READ_BUFF_SIZE);
	memset(toParseBuf, 0, REDIS_READ_BUFF_SIZE);
	toParseLen += REDIS_READ_BUFF_SIZE;
	mallocLen = toParseLen;
	while (!m_valid)
	{
		memset(recvBuf, 0, REDIS_READ_BUFF_SIZE);
		int32_t recvLen = m_socket.read(recvBuf, REDIS_READ_BUFF_SIZE - 1, m_readTimeout);
		if (recvLen < 0)
		{
			if (m_unparseBuf != NULL)
			{
				free(m_unparseBuf);
				m_unparseBuf = NULL;
			}
			if (toParseBuf != NULL)
			{
				free(toParseBuf);
				toParseBuf = NULL;
			}
			return false;
		}
		toParseLen = m_unparseLen + recvLen;
		if (m_unparseLen != 0)
		{
			if (m_unparseLen + recvLen >= mallocLen)
			{
				char* newBuf = (char*)malloc(mallocLen * 2);
				memset(newBuf, 0, mallocLen * 2);
				mallocLen *= 2;
				memcpy(newBuf, m_unparseBuf, m_unparseLen);
				memcpy(newBuf + m_unparseLen, recvBuf, recvLen);
				free(toParseBuf);
				toParseBuf = NULL;
				free(m_unparseBuf);
				m_unparseBuf = NULL;
				m_unparseLen = 0;
				//				toParseBuf = (char*)malloc(mallocLen);
				//				memset(toParseBuf, 0, mallocLen);
				//				memcpy(toParseBuf, newBuf, toParseLen);
				//				free(newBuf);
				toParseBuf = newBuf;
				newBuf = NULL;
			}
			else
			{
				memset(toParseBuf, 0, mallocLen);
				memcpy(toParseBuf, m_unparseBuf, m_unparseLen);
				memcpy(toParseBuf + m_unparseLen, recvBuf, recvLen);
				free(m_unparseBuf);
				m_unparseBuf = NULL;
				m_unparseLen = 0;
			}
		}
		else
		{
			memset(toParseBuf, 0, mallocLen);
			memcpy(toParseBuf, recvBuf, recvLen);
		}
		if (parserType == SCAN_PARSER)
			parseScanReply(toParseBuf, toParseLen, replyInfo);
		else
			parse(toParseBuf, toParseLen, replyInfo);
	}
	if (m_unparseBuf != NULL)
	{
		free(m_unparseBuf);
		m_unparseBuf = NULL;
	}
	if (toParseBuf != NULL)
	{
		free(toParseBuf);
		toParseBuf = NULL;
	}
	return m_valid;
}

bool RedisConnection::parseScanReply(char* parseBuf, int32_t parseLen, RedisReplyInfo& replyInfo)
{
    std::stringstream log_msg;
    log_msg << "parseScanReply, connection:[" << this << "] start to parse redis response:[" << parseBuf << "], parseLen: " << parseLen;
    LOG_WRITE_INFO(log_msg.str());

    const char* const end = parseBuf + parseLen;
    char* p = NULL;
    char buf[256]; // enough to contain key
    if (m_parseState == REDIS_PARSE_UNKNOWN_STATE && parseBuf != NULL)
    {
        m_parseState = REDIS_PARSE_TYPE;
    }
    while (parseBuf < end)
    {
        if (m_parseState == REDIS_PARSE_TYPE)
        {
            switch (*parseBuf)
            {
            case '-':
                m_parseState = REDIS_PARSE_RESULT;
                replyInfo.replyType = REDIS_REPLY_ERROR;
                parseBuf++;
                break;
            case '*': // assert *2
                m_parseState = REDIS_PARSE_ARRAYLENGTH;
                replyInfo.replyType = REDIS_REPLY_ARRAY;
                parseBuf++;
                break;
            default:
                m_valid = true;
                return false;
            }
        }
        else if (m_parseState == REDIS_PARSE_RESULT)
        {
            p = strstr(parseBuf, "\r\n");
            if (p != NULL) // get full error msg
            {
                memset(buf, 0, sizeof(buf));
                memcpy(buf, parseBuf, p - parseBuf);
                replyInfo.resultString = buf; // get error msg
                m_valid = true;
                return true;
            }
            else
            {
                goto check_buf;
            }
        }
        else if (m_parseState == REDIS_PARSE_ARRAYLENGTH)
        {
            p = strstr(parseBuf, "\r\n");
            if (p != NULL)
            {
                memset(buf, 0, sizeof(buf));
                memcpy(buf, parseBuf, p - parseBuf);
                int arrayNum = atoi(buf);
                if (arrayNum != 2)
                {
                    m_valid = true;
                    return false;
                }
                else
                {
                    m_parseState = REDIS_PARSE_CURSORLEN;
                    parseBuf = p + 2;
                }
            }
            else
            {
                goto check_buf;
            }
        }
        else if (m_parseState == REDIS_PARSE_CURSORLEN)
        {
            if (*parseBuf == '$') // TODO must be '*'
            {
                ++parseBuf;
            }
            else
            {
                m_valid = true;
                return false;
            }
            p = strstr(parseBuf, "\r\n");
            if (p != NULL)
            {
                memset(buf, 0, sizeof(buf));
                memcpy(buf, parseBuf, p - parseBuf);
                int cursorlen = atoi(buf);
                if (cursorlen < 0)
                {
                    m_valid = true;
                    return false;
                }
                else
                {
                    m_parseState = REDIS_PARSE_CURSOR;
                    parseBuf = p + 2;
                }
            }
            else
            {
                goto check_buf;
            }
        }
        else if (m_parseState == REDIS_PARSE_CURSOR)
        {
            p = strstr(parseBuf, "\r\n");
            if (p != NULL)
            {
                memset(buf, 0, sizeof(buf));
                memcpy(buf, parseBuf, p - parseBuf);
                int cursor = atoi(buf);
                if (cursor < 0)
                {
                    m_valid = true;
                    return false;
                }
                else
                {
                    m_parseState = REDIS_PARSE_KEYSLEN;
                    replyInfo.intValue = cursor; // get new cursor
                    parseBuf = p + 2;
                }
            }
            else
            {
                goto check_buf;
            }
        }
        else if (m_parseState == REDIS_PARSE_KEYSLEN)
        {
            if (*parseBuf != '*')
            {
                m_valid = true;
                return false;
            }
            parseBuf++;
            p = strstr(parseBuf, "\r\n");
            if (p != NULL)
            {
                memset(buf, 0, sizeof(buf));
                memcpy(buf, parseBuf, p - parseBuf);
                m_arrayNum = atoi(buf);
                if (m_arrayNum < 0)
                {
                    m_valid = true;
                    return false;
                }
                else if (m_arrayNum == 0)
                {
                    m_valid = true;
                    return true;
                }
                else
                {
                    m_parseState = REDIS_PARSE_KEYLEN;
                    parseBuf = p + 2;
                }
            }
            else
            {
                goto check_buf;
            }
        }
        else if (m_parseState == REDIS_PARSE_KEYLEN)
        {
            if (*parseBuf == '$')
            {
                ++parseBuf;
            }
            else
            {
                m_valid = true;
                return false;
            }
            p = strstr(parseBuf, "\r\n");
            if (p != NULL)
            {
                memset(buf, 0, sizeof(buf));
                memcpy(buf, parseBuf, p - parseBuf);
                int keylen = atoi(buf); // TODO save keylen in m_arrayLen               
                if (keylen < 0)
                {
                    m_valid = true;
                    return false;
                }
                else
                {
                    m_parseState = REDIS_PARSE_KEY;
                    parseBuf = p + 2;
                }
            }
            else
            {
                goto check_buf;
            }
        }
        else if (m_parseState == REDIS_PARSE_KEY)
        {
            p = strstr(parseBuf, "\r\n");
            if (p != NULL)
            {
                memset(buf, 0, sizeof(buf));
                memcpy(buf, parseBuf, p - parseBuf);
                int keylen = p - parseBuf; // TODO check keylen
                ReplyArrayInfo arrayInfo;
                arrayInfo.arrayLen = keylen;
                arrayInfo.arrayValue = (char*)malloc(arrayInfo.arrayLen + 1);
                memset(arrayInfo.arrayValue, 0, arrayInfo.arrayLen + 1);
                memcpy(arrayInfo.arrayValue, parseBuf, arrayInfo.arrayLen);
                arrayInfo.replyType = REDIS_REPLY_STRING;
                replyInfo.arrayList.push_back(arrayInfo);
                m_doneNum++; // TODO replyInfo.arrayList.size()

                if (m_doneNum < m_arrayNum)
                {
                    m_parseState = REDIS_PARSE_KEYLEN;
                    parseBuf += keylen + 2; // parseBuf = p+2;
                }
                else
                {
                    m_valid = true;
                    return true;
                }
            }
            else
            {
                goto check_buf;
            }
        }
        else
        {
            m_valid = true;
            return false;
        }
    }

check_buf:
    if (!m_valid)
    {
        if (end - parseBuf >= 1)
        {
            m_unparseLen = end - parseBuf;
            m_unparseBuf = (char*)malloc(m_unparseLen + 1);
            memset(m_unparseBuf, 0, m_unparseLen + 1);
            memcpy(m_unparseBuf, parseBuf, m_unparseLen);
        }
    }
    return true;
}

bool RedisConnection::parse(char* parseBuf, int32_t parseLen, RedisReplyInfo& replyInfo)
{
//  std::stringstream log_msg;
//  log_msg << "connection start to parse redis response:[" << parseBuf << "], parseLen: " << parseLen;
//  LOG_WRITE_INFO(log_msg.str());

    const char* const end = parseBuf + parseLen;
    char* p = NULL;
    if (m_parseState == REDIS_PARSE_UNKNOWN_STATE && parseBuf != NULL)
    {
        m_parseState = REDIS_PARSE_TYPE;
    }
    bool haveArray = false;
    char buf[4096];
    while (parseBuf < end)
    {
        switch (m_parseState)
        {
        case REDIS_PARSE_TYPE:
        {
            switch (*parseBuf)
            {
            case '-':
                m_parseState = REDIS_PARSE_RESULT;
                replyInfo.replyType = REDIS_REPLY_ERROR;
                break;
            case '+':
                m_parseState = REDIS_PARSE_RESULT;
                replyInfo.replyType = REDIS_REPLY_STATUS;
                break;
            case ':':
                m_parseState = REDIS_PARSE_INTEGER;
                replyInfo.replyType = REDIS_REPLY_INTEGER;
                break;
            case '$':
                m_parseState = REDIS_PARSE_LENGTH;
                replyInfo.replyType = REDIS_REPLY_STRING;
                m_arrayNum = 1;
                break;
            case '*':
                m_parseState = REDIS_PARSE_ARRAYLENGTH;
                replyInfo.replyType = REDIS_REPLY_ARRAY;
                break;
            default:
                replyInfo.replyType = REDIS_REPLY_UNKNOWN;
                return false;

            }
        }
        break;
        case REDIS_PARSE_INTEGER:
        {
            p = strstr(parseBuf, "\r\n");
            if (p != NULL)
            {
                memset(buf, 0, 4096);
                memcpy(buf, parseBuf, p - parseBuf);
                replyInfo.intValue = atoi(buf);
                m_valid = true;
                parseBuf = p;
                //parse '\r'
                ++parseBuf;
            }
            else
            {
                goto check_buf;
            }
        }
        break;
        case REDIS_PARSE_RESULT:
        {
            p = strstr(parseBuf, "\r\n");
            if (p != NULL)
            {
                memset(buf, 0, 4096);
                memcpy(buf, parseBuf, p - parseBuf);
                replyInfo.resultString = buf;
                m_valid = true;
                parseBuf = p;
                //parse '\r'
                ++parseBuf;
            }
            else
            {
                goto check_buf;
            }
        }
        break;
        case REDIS_PARSE_LENGTH:
        {
            if (haveArray && (*parseBuf == '-' || *parseBuf == '+' || *parseBuf == ':'))
            {
                p = strstr(parseBuf, "\r\n");
                if (p != NULL)
                {
                    ReplyArrayInfo arrayInfo;
                    arrayInfo.arrayLen = p - parseBuf;
                    if (arrayInfo.arrayLen <= 0)
                    {
                        goto check_buf;
                    }
                    arrayInfo.replyType = REDIS_REPLY_STRING;
                    arrayInfo.arrayValue = (char*)malloc(arrayInfo.arrayLen + 1);
                    //for string last char
                    memset(arrayInfo.arrayValue, 0, arrayInfo.arrayLen + 1);
                    memcpy(arrayInfo.arrayValue, parseBuf, arrayInfo.arrayLen);
                    replyInfo.arrayList.push_back(arrayInfo);
                    m_doneNum++;
                    if (m_doneNum < m_arrayNum)
                    {
                        m_parseState = REDIS_PARSE_LENGTH;
                    }
                    else
                    {
                        m_valid = true;
                    }
                    parseBuf = p;
                    //parse '\r'
                    ++parseBuf;
                }
                else
                {
                    goto check_buf;
                }
                break;
            }
            //for array data,may be first is $
            if (*parseBuf == '$')
            {
                ++parseBuf;
            }
            p = strstr(parseBuf, "\r\n");
            if (p != NULL)
            {
                memset(buf, 0, 4096);
                memcpy(buf, parseBuf, p - parseBuf);
                m_arrayLen = atoi(buf);
                parseBuf = p;
                //parse '\r'
                ++parseBuf;
                if (m_arrayLen != -1)
                {
                    m_parseState = REDIS_PARSE_STRING;
                }
                else
                {
                    ReplyArrayInfo arrayInfo;
                    arrayInfo.arrayLen = -1;
                    arrayInfo.replyType = REDIS_REPLY_NIL;
                    replyInfo.arrayList.push_back(arrayInfo);
                    m_doneNum++;
                    if (m_doneNum < m_arrayNum)
                    {
                        m_parseState = REDIS_PARSE_LENGTH;
                    }
                    else
                    {
                        m_valid = true;
                    }
                }
            }
            else
            {
                goto check_buf;
            }
        }
        break;
        case REDIS_PARSE_STRING:
        {
            //can not use strstr,for maybe binary data.
            //fix for if not recv \r\n,must recv \r\n.
            if (end - parseBuf >= (m_arrayLen + 2))
            {
                ReplyArrayInfo arrayInfo;
                arrayInfo.arrayLen = m_arrayLen;
                arrayInfo.arrayValue = (char*)malloc(arrayInfo.arrayLen + 1);
                //for string last char
                memset(arrayInfo.arrayValue, 0, arrayInfo.arrayLen + 1);
                memcpy(arrayInfo.arrayValue, parseBuf, arrayInfo.arrayLen);
                arrayInfo.replyType = REDIS_REPLY_STRING;
                replyInfo.arrayList.push_back(arrayInfo);
                m_doneNum++;
                parseBuf += m_arrayLen;
                //parse '\r'
                ++parseBuf;
                if (m_doneNum < m_arrayNum)
                {
                    m_parseState = REDIS_PARSE_LENGTH;
                }
                else
                {
                    m_valid = true;
                }
            }
            else
            {
                goto check_buf;
            }
        }
        break;
        case REDIS_PARSE_ARRAYLENGTH:
        {
            p = strstr(parseBuf, "\r\n");
            if (p != NULL)
            {
                memset(buf, 0, 4096);
                memcpy(buf, parseBuf, p - parseBuf);
                m_arrayNum = atoi(buf);
                parseBuf = p;
                //parse '\r'
                ++parseBuf;
                if (m_arrayNum == 0)
                {
                    m_valid = true;
                }
                else
                {
                    //add for exec failed reply.
                    if (m_arrayNum == -1)
                    {
                        m_valid = true;
                        replyInfo.intValue = -1;
                    }
                    else
                    {
                        m_parseState = REDIS_PARSE_LENGTH;
                        haveArray = true;
                    }
                }
            }
            else
            {
                goto check_buf;
            }
        }
        break;
        }
        ++parseBuf;
    }

check_buf:
    if (!m_valid)
    {
        if (end - parseBuf >= 1)
        {
            m_unparseLen = end - parseBuf;
            m_unparseBuf = (char*)malloc(m_unparseLen + 1);
            memset(m_unparseBuf, 0, m_unparseLen + 1);
            memcpy(m_unparseBuf, parseBuf, m_unparseLen);
        }
    }
    return true;
}

void RedisConnection::checkConnectionStatus()
{
	//check if the socket is closed by peer, maybe it's in CLOSE_WAIT state
	if (m_socket.fd >= 0)
	{
		unsigned char buf[1];
		int flags = fcntl(m_socket.fd, F_GETFL, 0);
		fcntl(m_socket.fd, F_SETFL, flags | O_NONBLOCK);
		int nRead = ::read(m_socket.fd, buf, sizeof(buf));
		if (nRead == 0)
		{
			std::stringstream log_msg;
			log_msg << "connection:[" << this << "] the connection to " << m_socket.m_connectToHost << ":" << m_socket.m_connectToPort << " has been closed by peer before";
			LOG_WRITE_WARNING(log_msg.str());
			m_socket.close();
		}
		fcntl(m_socket.fd, F_SETFL, flags);
	}
}

bool RedisConnection::AuthPasswd()
{
    list<RedisCmdParaInfo> paraList;
    int32_t paraLen = 0; 
    RedisClient::fillCommandPara("auth", 4, paraList);
    paraLen += 15;
    RedisClient::fillCommandPara(m_passwd.c_str(), m_passwd.length(), paraList);
    paraLen += m_passwd.length() + 20;

    RedisReplyInfo replyInfo;
    bool success = doRedisCommand(paraList, paraLen, replyInfo);
    RedisClient::freeCommandList(paraList);
    if (!success)
        return false;
    success = RedisClient::ParseAuthReply(replyInfo);
    RedisClient::freeReplyInfo(replyInfo);
    if (success)
    {    
        std::stringstream log_msg;
        log_msg << "when connect, auth passwd " << m_passwd << " success";
        LOG_WRITE_INFO(log_msg.str());
    }
	else
	{
        std::stringstream log_msg;
        log_msg << "when connect, auth passwd " << m_passwd << " failed";
        LOG_WRITE_ERROR(log_msg.str());
	}
    return success;
}

int RedisConnection::GetSockfd()
{
	return m_socket.getFileDescriptor();
}

} // namespace GBDownLinker
