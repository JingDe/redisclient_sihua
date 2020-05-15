#include "redisconnection.h"
#include "redisclient.h"
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <cassert>
#include <sstream>
#include "base_library/log.h"

namespace GBDownLinker {

typedef bool (*ParseFunction)(void* parseBuf, int32_t parseLen, RedisReplyInfo& replyInfo);

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

bool RedisConnection::doCommandWithParseEnhance(list < RedisCmdParaInfo >& paraList, int32_t paraLen, CommonReplyInfo& replyInfo)
{
	checkConnectionStatus();
	if (m_socket.fd == INVALID_SOCKET_HANDLE)
	{
		std::stringstream log_msg;
		log_msg << "connection:[" << this << "] socket may be closed by peer.";
		LOG_WRITE_ERROR(log_msg.str());
		if (!connect())
		{
			std::stringstream log_msg;
			log_msg << "connection:[" << this << "] reconnect to server failed.";
			LOG_WRITE_ERROR(log_msg.str());
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
	return recvWithParseEnhance(replyInfo);
}


bool RedisConnection::recvWithParseEnhance(CommonReplyInfo& replyInfo)
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
			std::stringstream log_msg;
			log_msg << "connection:[" << this << "] failed to read socket";
			LOG_WRITE_ERROR(log_msg.str());
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
		parseEnhance(toParseBuf, toParseLen, replyInfo);
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

// test this with "sentinel slaves" in RedisClient::DoTestOfSentinelSlavesCommand
// can deal with reply of int/string/array/vector of vector<ReplyArrayInfo>. (ReplyArrayInfo=string)
// can not deal with "scan" command, like vector contains string+vector<ReplyArrayInfo>+other_type.
bool RedisConnection::parseEnhance(char* parseBuf, int32_t parseLen, CommonReplyInfo& replyInfo)
{
	std::stringstream log_msg;
	log_msg << "parseEnhance, connection:[" << this << "] start to parse redis response:[" << parseBuf << "], parseLen: " << parseLen;
	LOG_WRITE_INFO(log_msg.str());

	const char* const end = parseBuf + parseLen;
	bool haveArray = false;
	char* p = NULL;
	char buf[4096];
	if (m_parseState == REDIS_PARSE_UNKNOWN_STATE && parseBuf != NULL)
	{
		m_parseState = REDIS_PARSE_TYPE;
		replyInfo.arrays_size = 0;
		replyInfo.cur_array_pos = replyInfo.arrays_size;
		replyInfo.cur_array_size = 0;
	}
	while (parseBuf < end)
	{
		if (m_parseState == REDIS_PARSE_TYPE)
		{
			switch (*parseBuf)
			{
			case '-':
				m_parseState = REDIS_PARSE_RESULT_ERR;
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_ERROR;
				//				LOG(WARNING)<<"reply ERR";
				parseBuf++;
				break;
			case '+':
				m_parseState = REDIS_PARSE_RESULT_OK;
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_STATUS;
				break;
			case ':':
				m_parseState = REDIS_PARSE_INTEGER;
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_INTEGER;
				break;
			case '$':
				m_parseState = REDIS_PARSE_LENGTH;
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;
				m_arrayNum = 1;
				replyInfo.cur_array_size = 1; // 当reply只有一个string时，存储在第一个Array中？
				replyInfo.cur_array_pos = 0;
				replyInfo.arrays_size = 1; // TODO 
				replyInfo.arrays.resize(replyInfo.arrays_size);
				break;
			case '*':
			{
				// 尝试获取前两行，检查第二行是否*开头，是则表示多个Array，否则表示一个Array
//				LOG(INFO)<<"get reply type ARRAY, must check array size";					
				parseBuf++;

				// 尝试找当前*之后的\r\n，紧接着的符号，是否*
				char* tmp = parseBuf;
				while (tmp + 3 <= end)
				{
					if (*tmp == '\r' && *(tmp + 1) == '\n')
						break;
					tmp++;
				}

				if (tmp + 3 > end)
				{
					//					LOG(INFO)<<"wait more data";
					m_parseState = REDIS_PARSE_CHECK_ARRAYS_SIZE;
					m_valid = false;
					goto check_buf;
				}
				assert(*tmp == '\r' && *(tmp + 1) == '\n');
				char second_line_start_charactor = *(tmp + 2);
				if (second_line_start_charactor == '*')
				{
					//					LOG(INFO)<<"reply has multi array";
					replyInfo.replyType = RedisReplyType::REDIS_REPLY_MULTI_ARRRY;
					replyInfo.arrays_size = atoi(string(parseBuf, tmp - parseBuf).c_str());
					//					LOG(INFO)<<"arrays size is "<<replyInfo.arrays_size;

										// 下面解析第一个Array数组
					replyInfo.arrays.resize(replyInfo.arrays_size);
					replyInfo.cur_array_pos = 0;
					parseBuf = tmp + 3;
					m_parseState = REDIS_PARSE_ARRAYLENGTH;
				}
				else
				{
					//					LOG(INFO)<<"reply has one array";
					replyInfo.replyType = RedisReplyType::REDIS_REPLY_ARRAY;
					replyInfo.arrays_size = 1;

					replyInfo.cur_array_size = atoi(string(parseBuf, tmp - parseBuf).c_str());
					// 获得唯一的Array数组的长度
//					LOG(INFO)<<"the array size is "<<replyInfo.cur_array_size;
					m_arrayNum = replyInfo.cur_array_size;

					// 下面解析唯一的Array数组
					replyInfo.arrays.resize(replyInfo.arrays_size);
					replyInfo.cur_array_pos = 0;
					parseBuf = tmp + 3;
					m_parseState = REDIS_PARSE_LENGTH;
				}
			}
			break;
			default:
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_UNKNOWN;
				//				LOG(ERROR)<<"recv unknown type redis response.";					
				//				LOG(ERROR)<<"parse type error, "<<*parseBuf<<", "<<parseBuf;
				m_valid = true;
				return false;
			}
		}
		else if (m_parseState == REDIS_PARSE_INTEGER)
		{
			p = strstr(parseBuf, "\r\n");
			if (p != NULL)
			{
				memset(buf, 0, sizeof(buf) / sizeof(buf[0]));
				memcpy(buf, parseBuf, p - parseBuf);
				replyInfo.intValue = atoi(buf);
				//				m_valid = true;
				if (replyInfo.cur_array_pos >= replyInfo.arrays_size)
				{
					//					LOG(INFO)<<"get integer ok: "<<replyInfo.intValue<<", and no more Array";
					m_valid = true;
				}
				else
				{
					m_parseState = REDIS_PARSE_ARRAYLENGTH;
				}

				parseBuf = p;
				//parse '\r'
				++parseBuf;
				++parseBuf;
			}
			else
			{
				goto check_buf;
			}
		}
		else if (m_parseState == REDIS_PARSE_CHECK_ARRAYS_SIZE) // 等待第二行的首字符
		{
			// 同上
			// 尝试找当前*之后的\r\n，紧接着的符号，是否*
			char* tmp = parseBuf;
			while (tmp + 3 <= end)
			{
				if (*tmp == '\r' && *(tmp + 1) == '\n')
					break;
				tmp++;
			}

			if (tmp + 3 > end)
			{
				//				LOG(INFO)<<"wait more data";
				m_parseState = REDIS_PARSE_CHECK_ARRAYS_SIZE;
				m_valid = false;
				goto check_buf;
			}
			assert(*tmp == '\r' && *(tmp + 1) == '\n');
			char second_line_start_charactor = *(tmp + 2);
			if (second_line_start_charactor == '*')
			{
				//				LOG(INFO)<<"reply has multi array";
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_MULTI_ARRRY;
				replyInfo.arrays_size = atoi(string(parseBuf, tmp - parseBuf).c_str());
				//				LOG(INFO)<<"arrays size is "<<replyInfo.arrays_size;

								// 下面解析第一个Array数组
				replyInfo.arrays.resize(replyInfo.arrays_size);
				replyInfo.cur_array_pos = 0;
				parseBuf = tmp + 3;
				m_parseState = REDIS_PARSE_ARRAYLENGTH;
			}
			else
			{
				//				LOG(INFO)<<"reply has one array";
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_ARRAY;
				replyInfo.arrays_size = 1;

				replyInfo.cur_array_size = atoi(string(parseBuf, tmp - parseBuf).c_str());
				// 获得唯一的Array数组的长度
//				LOG(INFO)<<"the array size is "<<replyInfo.cur_array_size;
				m_arrayNum = replyInfo.cur_array_size;

				// 下面解析唯一的Array数组
				replyInfo.arrays.resize(replyInfo.arrays_size);
				replyInfo.cur_array_pos = 0;
				parseBuf = tmp + 3;
				m_parseState = REDIS_PARSE_LENGTH;
			}
		}
		else if (m_parseState == REDIS_PARSE_RESULT_OK
			|| m_parseState == REDIS_PARSE_RESULT_ERR) // ERR OK
		{
			p = strstr(parseBuf, "\r\n");
			if (p != NULL) // get full error msg
			{
				memset(buf, 0, sizeof(buf) / sizeof(buf[0]));
				memcpy(buf, parseBuf, p - parseBuf);
				replyInfo.resultString = buf; // get error msg
//				LOG(INFO)<<"parse get result: "<<buf;
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
			m_doneNum = 0; // 当前已经解析到的数组元素的个数，与 m_arrayNum 比较
			// 或者通过 replyInfo.arrays[replyInfo.cur_array_pos].size(); 比较 replyInfo.cur_array_size

			p = strstr(parseBuf, "\r\n");
			if (p != NULL)
			{
				memset(buf, 0, sizeof(buf) / sizeof(buf[0]));
				memcpy(buf, parseBuf, p - parseBuf);
				m_arrayNum = atoi(buf);
				replyInfo.cur_array_size = m_arrayNum;
				//				LOG(INFO)<<"get one Array size "<<replyInfo.cur_array_size<<", pos is "<<replyInfo.cur_array_pos<<", total is "<<replyInfo.arrays_size;
				parseBuf = p;
				//parse '\r'
				++parseBuf;	++parseBuf;
				if (m_arrayNum == 0)
				{
					m_valid = true;
					// TODO 是否解析下一个Array
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
		else if (m_parseState == REDIS_PARSE_LENGTH)
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
					arrayInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;
					arrayInfo.arrayValue = (char*)malloc(arrayInfo.arrayLen + 1);
					//for string last char
					memset(arrayInfo.arrayValue, 0, arrayInfo.arrayLen + 1);
					memcpy(arrayInfo.arrayValue, parseBuf, arrayInfo.arrayLen);

					//					if(arrayInfo.arrayLen)
					{
						replyInfo.arrays[replyInfo.cur_array_pos].push_back(arrayInfo);
						//						LOG(INFO)<<"get Array "<<replyInfo.cur_array_pos<<" elem: ["<<arrayInfo.arrayValue<<"]";
						m_doneNum++;
					}
					//					else
					//					{
					//						LOG(WARNING)<<"why string empty";
					//					}					

					if (m_doneNum < m_arrayNum)
					{
						m_parseState = REDIS_PARSE_LENGTH;
					}
					else
					{
						//						LOG(INFO)<<"ok get one Array of size "<<replyInfo.arrays[replyInfo.cur_array_pos].size();
						if (replyInfo.cur_array_pos + 1 < replyInfo.arrays_size)
						{
							//							LOG(INFO)<<"now get "<<replyInfo.cur_array_pos+1<<" Array, total "<<replyInfo.arrays_size<<" Array";
							m_parseState = REDIS_PARSE_ARRAYLENGTH; // 解析下一个数组的长度
							replyInfo.cur_array_pos++;
							m_doneNum = 0;
							m_arrayNum = 0;
						}
						else
						{
							m_valid = true;
							//							LOG(INFO)<<"get all "<<replyInfo.arrays_size<<" Array ok";
						}
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
				memset(buf, 0, sizeof(buf) / sizeof(buf[0]));
				memcpy(buf, parseBuf, p - parseBuf);
				m_arrayLen = atoi(buf);
				parseBuf = p;
				//parse '\r'
				++parseBuf;	++parseBuf;
				if (m_arrayLen != -1)
				{
					m_parseState = REDIS_PARSE_STRING;
				}
				else
				{
					ReplyArrayInfo arrayInfo;
					arrayInfo.arrayLen = -1;
					arrayInfo.replyType = RedisReplyType::REDIS_REPLY_NIL;
					replyInfo.arrays[replyInfo.cur_array_pos].push_back(arrayInfo);
					//					LOG(WARNING)<<"get Array "<<replyInfo.cur_array_pos<<" elem: [nil]";

					m_doneNum++;
					if (m_doneNum < m_arrayNum)
					{
						m_parseState = REDIS_PARSE_LENGTH;
					}
					else
					{
						//						LOG(INFO)<<"ok get one Array of size "<<replyInfo.arrays[replyInfo.cur_array_pos].size();
						if (replyInfo.cur_array_pos + 1 < replyInfo.arrays_size)
						{
							//							LOG(INFO)<<"now get "<<replyInfo.cur_array_pos+1<<" Array, total "<<replyInfo.arrays_size<<" Array";
							m_parseState = REDIS_PARSE_ARRAYLENGTH; // 解析下一个数组的长度
							replyInfo.cur_array_pos++;
							m_doneNum = 0;
							m_arrayNum = 0;
						}
						else
						{
							m_valid = true;
							//							LOG(INFO)<<"get all "<<replyInfo.arrays_size<<" Array ok";
						}
					}
				}
			}
			else
			{
				goto check_buf;
			}
		}
		else if (m_parseState == REDIS_PARSE_STRING)
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
				arrayInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;

				//				if(arrayInfo.arrayLen>0)
				{
					replyInfo.arrays[replyInfo.cur_array_pos].push_back(arrayInfo);
					//					LOG(INFO)<<"get Array "<<replyInfo.cur_array_pos<<" elem: ["<<arrayInfo.arrayValue<<"]";
					m_doneNum++;
				}			

				parseBuf += m_arrayLen;
				//parse '\r'
				++parseBuf;	++parseBuf;
				if (m_doneNum < m_arrayNum)
				{
					m_parseState = REDIS_PARSE_LENGTH;
				}
				else
				{
					//					LOG(INFO)<<"ok get one Array of size "<<replyInfo.arrays[replyInfo.cur_array_pos].size();
					if (replyInfo.cur_array_pos + 1 < replyInfo.arrays_size)
					{
						//						LOG(INFO)<<"now get "<<replyInfo.cur_array_pos+1<<" Array, total "<<replyInfo.arrays_size;
						m_parseState = REDIS_PARSE_ARRAYLENGTH; // 解析下一个数组的长度
						parseBuf++;
						replyInfo.cur_array_pos++;
						m_doneNum = 0;
						m_arrayNum = 0;
					}
					else
					{
						m_valid = true;
						//						LOG(INFO)<<"get all "<<replyInfo.arrays_size<<" Array ok";
					}
				}
			}
			else
			{
				goto check_buf;
			}
		}
		else
		{
			//			LOG(ERROR)<<"unknown parse state "<<m_parseState;
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

//// TODO
//// try to deal with reply of command like "scan", 
//// return a vector contains string+vector<ReplyArrayInfo>+other_type.
//bool RedisConnection::parseEnhance2(char *parseBuf, int32_t parseLen, CommonReplyInfo2 & replyInfo)
//{
//	LOG(INFO)<<"parseEnhance, connection:["<<this<<"] start to parse redis response:["<<parseBuf<<"], parseLen: "<<parseLen;
//	const char * const end = parseBuf + parseLen;
//	bool haveArray = false;
//	char *p=NULL;
//	char buf[4096];
//	if (m_parseState == REDIS_PARSE_UNKNOWN_STATE && parseBuf != NULL)
//	{
//		m_parseState = REDIS_PARSE_TYPE;
//		replyInfo.arrays_size=0;
//		replyInfo.cur_array_pos=replyInfo.arrays_size;
//		replyInfo.cur_array_size=0;
//	}
//	while(parseBuf < end)
//	{
//		if(m_parseState==REDIS_PARSE_TYPE)
//		{
//			switch(*parseBuf)
//			{
//			case '-':
//				m_parseState = REDIS_PARSE_RESULT_ERR;
//				replyInfo.replyType = RedisReplyType::REDIS_REPLY_ERROR;
//				LOG(WARNING)<<"reply ERR";
//				parseBuf++;
//				break;
//			case '+':
//				m_parseState = REDIS_PARSE_RESULT_OK;
//				replyInfo.replyType = RedisReplyType::REDIS_REPLY_STATUS;
//				break;
//			case ':':
//				m_parseState = REDIS_PARSE_INTEGER;
//				replyInfo.replyType = RedisReplyType::REDIS_REPLY_INTEGER;
//				break;
//			case '$':
//				m_parseState = REDIS_PARSE_LENGTH;
//				replyInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;
//				m_arrayNum = 1;
//				replyInfo.cur_array_size=1; // 当reply只有一个string时，存储在第一个Array中？
//				replyInfo.cur_array_pos=0;
//				replyInfo.arrays_size=1; // TODO 
//				replyInfo.arrays.resize(replyInfo.arrays_size);
//				break;
//			case '*':
//				{
//				// 尝试获取前两行，检查第二行是否*开头，是则表示多个Array，否则表示一个Array
//				LOG(INFO)<<"get reply type ARRAY, must check array size";					
//				parseBuf++;
//
//				// 获得*后的数组，即顶层数组arrays的大小
//				char* tmp=parseBuf;
//				while(tmp+3<=end)
//				{
//					if(*tmp=='\r'  &&  *(tmp+1)=='\n')
//						break;
//					tmp++;
//				}
//
//				if(tmp+3>end)
//				{
//					LOG(INFO)<<"wait more data";
//					m_parseState=REDIS_PARSE_CHECK_ARRAYS_SIZE;
//					m_valid=false;
//					goto check_buf;
//				}
//				assert(*tmp=='\r'  &&  *(tmp+1)=='\n');
//				replyInfo.arrays_size=atoi(string(parseBuf, tmp-parseBuf).c_str());
//				replyInfo.cur_array_pos=0;
//				LOG(INFO)<<"reply arrays_size is "<<replyInfo.arrays_size;
//				m_parseState=REDIS_PARSE_ARRAYLENGTH; // to get replyInfo.cur_array_size
//				
//				}
//				break;
//			default:
//				replyInfo.replyType = RedisReplyType::REDIS_REPLY_UNKNOWN;
//				LOG(ERROR)<<"recv unknown type redis response.";					
//				LOG(ERROR)<<"parse type error, "<<*parseBuf<<", "<<parseBuf;
//				m_valid=true;
//				return false;
//			}
//		}
//		else if(m_parseState==REDIS_PARSE_INTEGER)
//		{
//			p = strstr(parseBuf, "\r\n");
//			if (p != NULL)
//			{
//				memset(buf, 0, sizeof(buf)/sizeof(buf[0]));
//				memcpy(buf, parseBuf, p-parseBuf);
//				replyInfo.intValue = atoi(buf);
//
//				if(replyInfo.cur_array_pos>=replyInfo.arrays_size)
//				{
//					LOG(INFO)<<"get integer ok: "<<replyInfo.intValue<<", and no more Array";
//					m_valid=true;
//				}
//				else
//				{
//					m_parseState=REDIS_PARSE_ARRAYLENGTH;
//				}
//
//				parseBuf = p;
//				//parse '\r'
//				++parseBuf; 
//				++parseBuf;
//			}
//			else
//			{
//				goto check_buf;
//			}
//		}
//		else if(m_parseState==REDIS_PARSE_CHECK_ARRAYS_SIZE) // 用于获得顶层数组arrays的长度
//		{
//			// 同上
//			
//		}
//		else if(m_parseState==REDIS_PARSE_RESULT_OK  
//			||  m_parseState==REDIS_PARSE_RESULT_ERR) // ERR OK
//		{			
//			p = strstr(parseBuf, "\r\n");
//			if (p != NULL) // get full error msg
//			{
//				memset(buf, 0, sizeof(buf)/sizeof(buf[0]));
//				memcpy(buf, parseBuf, p-parseBuf);
//				replyInfo.resultString = buf; // get error msg
//				LOG(INFO)<<"parse get result: "<<buf;
//				m_valid = true;
//				return true;
//			}
//			else
//			{
//				goto check_buf;
//			}
//		}		
//		else if(m_parseState==REDIS_PARSE_ARRAYLENGTH) // 用于获得二级/子数组的长度
//		{
//			if(*parseBuf!='*')
//			{
//				assert(*parseBuf=='$');
//				parseBuf++;
//				m_parseState=REDIS_PARSE_LENGTH;
//				replyInfo.cur_array_size=1;
//				m_arrayNum=1;
//				m_doneNum=0;
//				LOG(INFO)<<"current char is "<<*parseBuf<<", is type of arrays["<<replyInfo.cur_array_pos<<"]";
//				continue;
//			}
//
//			LOG(INFO)<<"reply arrays["<<replyInfo.cur_array_pos<<"] is really array type";
//			
//			m_doneNum=0; // 当前已经解析到的数组元素的个数，与 m_arrayNum 比较
//			// 或者通过 replyInfo.arrays[replyInfo.cur_array_pos].size(); 比较 replyInfo.cur_array_size
//			
//			p = strstr(parseBuf, "\r\n");
//			if (p != NULL)
//			{
//				memset(buf, 0, sizeof(buf)/sizeof(buf[0]));
//				memcpy(buf, parseBuf, p-parseBuf);
//				m_arrayNum = atoi(buf);
//				replyInfo.cur_array_size=m_arrayNum;
//				LOG(INFO)<<"get one Array size "<<replyInfo.cur_array_size<<", pos is "<<replyInfo.cur_array_pos<<", total is "<<replyInfo.arrays_size;
//				parseBuf = p;
//				//parse '\r'
//				++parseBuf;	++parseBuf;
//				if (m_arrayNum == 0)
//				{
//					m_valid = true;
//					// TODO 是否解析下一个Array
//				}
//				else
//				{
//					//add for exec failed reply.
//					if (m_arrayNum == -1)
//					{
//						m_valid = true;
//						replyInfo.intValue = -1;
//					}
//					else
//					{
//						m_parseState = REDIS_PARSE_LENGTH;
//						haveArray = true;
//					}
//				}
//			}
//			else
//			{
//				goto check_buf;
//			}
//		}		
//		else if(m_parseState==REDIS_PARSE_LENGTH)
//		{
//			if (haveArray && (*parseBuf == '-' || *parseBuf == '+' || *parseBuf == ':'))
//			{
//				p = strstr(parseBuf, "\r\n");
//				if (p != NULL)
//				{
//					ReplyArrayInfo arrayInfo;
//					arrayInfo.arrayLen = p-parseBuf;
//					if (arrayInfo.arrayLen <= 0)
//					{
//						goto check_buf;
//					}
//					arrayInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;
//					arrayInfo.arrayValue = (char*)malloc(arrayInfo.arrayLen+1);
//					//for string last char
//					memset(arrayInfo.arrayValue, 0, arrayInfo.arrayLen+1);
//					memcpy(arrayInfo.arrayValue, parseBuf, arrayInfo.arrayLen);
//
////					if(arrayInfo.arrayLen)
//					{
////						replyInfo.arrays[replyInfo.cur_array_pos].push_back(arrayInfo);
//						replyInfo.arrays[replyInfo.cur_array_pos]
//						LOG(INFO)<<"get Array "<<replyInfo.cur_array_pos<<" elem: ["<<arrayInfo.arrayValue<<"]";
//						m_doneNum++;
//					}
////					else
////					{
////						LOG(WARNING)<<"why string empty";
////					}					
//					
//					if (m_doneNum < m_arrayNum)
//					{
//						m_parseState = REDIS_PARSE_LENGTH;
//					}
//					else
//					{
//						LOG(INFO)<<"ok get one Array of size "<<replyInfo.arrays[replyInfo.cur_array_pos].size();
//						if(replyInfo.cur_array_pos+1<replyInfo.arrays_size)
//						{							
//							LOG(INFO)<<"now get "<<replyInfo.cur_array_pos+1<<" Array, total "<<replyInfo.arrays_size<<" Array";
//							m_parseState=REDIS_PARSE_ARRAYLENGTH; // 解析下一个数组的长度
//							replyInfo.cur_array_pos++;
//							m_doneNum=0;
//							m_arrayNum=0;
//						}
//						else
//						{
//							m_valid = true;
//							LOG(INFO)<<"get all "<<replyInfo.arrays_size<<" Array ok";
//						}
//					}
//					parseBuf = p;
//					//parse '\r'
//					++parseBuf;
//				}
//				else
//				{
//					goto check_buf;
//				}
//				break;
//			}
//			//for array data,may be first is $
//			if (*parseBuf == '$')
//			{
//				++parseBuf;
//			}
//			p = strstr(parseBuf, "\r\n");
//			if (p != NULL)
//			{
//				memset(buf, 0, sizeof(buf)/sizeof(buf[0]));
//				memcpy(buf, parseBuf, p-parseBuf);
//				m_arrayLen = atoi(buf);
//				parseBuf = p;
//				//parse '\r'
//				++parseBuf;	++parseBuf;
//				if (m_arrayLen != -1)
//				{
//					m_parseState = REDIS_PARSE_STRING;
//				}
//				else
//				{
//					ReplyArrayInfo arrayInfo;
//					arrayInfo.arrayLen = -1;
//					arrayInfo.replyType = RedisReplyType::REDIS_REPLY_NIL;
//					replyInfo.arrays[replyInfo.cur_array_pos].push_back(arrayInfo);
//					LOG(WARNING)<<"get Array "<<replyInfo.cur_array_pos<<" elem: [nil]";
//					
//					m_doneNum++;
//					if (m_doneNum < m_arrayNum)
//					{
//						m_parseState = REDIS_PARSE_LENGTH;
//					}
//					else
//					{
//						LOG(INFO)<<"ok get one Array of size "<<replyInfo.arrays[replyInfo.cur_array_pos].size();
//						if(replyInfo.cur_array_pos+1<replyInfo.arrays_size)
//						{							
//							LOG(INFO)<<"now get "<<replyInfo.cur_array_pos+1<<" Array, total "<<replyInfo.arrays_size<<" Array";
//							m_parseState=REDIS_PARSE_ARRAYLENGTH; // 解析下一个数组的长度
//							replyInfo.cur_array_pos++;
//							m_doneNum=0;
//							m_arrayNum=0;
//						}
//						else
//						{
//							m_valid = true;
//							LOG(INFO)<<"get all "<<replyInfo.arrays_size<<" Array ok";
//						}
//					}
//				}
//			}
//			else
//			{
//				goto check_buf;
//			}
//		}
//		else if(m_parseState==REDIS_PARSE_STRING)
//		{
//			//can not use strstr,for maybe binary data.
//			//fix for if not recv \r\n,must recv \r\n.
//			if (end-parseBuf >= (m_arrayLen+2))
//			{
//				ReplyArrayInfo arrayInfo;
//				arrayInfo.arrayLen = m_arrayLen;
//				arrayInfo.arrayValue = (char*)malloc(arrayInfo.arrayLen+1);
//				//for string last char
//				memset(arrayInfo.arrayValue, 0, arrayInfo.arrayLen+1);
//				memcpy(arrayInfo.arrayValue, parseBuf, arrayInfo.arrayLen);
//				arrayInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;
//
////				if(arrayInfo.arrayLen>0)
//				{
//					replyInfo.arrays[replyInfo.cur_array_pos].push_back(arrayInfo);
//					LOG(INFO)<<"get Array "<<replyInfo.cur_array_pos<<" elem: ["<<arrayInfo.arrayValue<<"]";
//					m_doneNum++;
//				}
////				else
////				{
////					LOG(WARNING)<<"why string empty";
////				}				
//				
//				parseBuf += m_arrayLen;
//				//parse '\r'
//				++parseBuf;	++parseBuf;
//				if (m_doneNum < m_arrayNum)
//				{
//					m_parseState = REDIS_PARSE_LENGTH;
//				}
//				else
//				{
//					LOG(INFO)<<"ok get one Array of size "<<replyInfo.arrays[replyInfo.cur_array_pos].size();
//					if(replyInfo.cur_array_pos+1<replyInfo.arrays_size)
//					{						
//						LOG(INFO)<<"now get "<<replyInfo.cur_array_pos+1<<" Array, total "<<replyInfo.arrays_size;
//						m_parseState=REDIS_PARSE_ARRAYLENGTH; // 解析下一个数组的长度
//						parseBuf++;
//						replyInfo.cur_array_pos++;
//						m_doneNum=0;
//						m_arrayNum=0;
//					}
//					else
//					{
//						m_valid = true;
//						LOG(INFO)<<"get all "<<replyInfo.arrays_size<<" Array ok";
//					}
//				}
//			}
//			else
//			{
//				goto check_buf;
//			}
//		}
//		else
//		{
//			LOG(ERROR)<<"unknown parse state "<<m_parseState;
//			m_valid=true;
//			return false;
//		}
//	}
//
//check_buf:
//	if (!m_valid)
//	{
//		if (end-parseBuf >= 1)
//		{
//			m_unparseLen = end-parseBuf;
//			m_unparseBuf = (char*)malloc(m_unparseLen+1);
//			memset(m_unparseBuf, 0, m_unparseLen+1);
//			memcpy(m_unparseBuf, parseBuf, m_unparseLen);
//		}
//	}
//	return true;
//}


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
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_ERROR;
				parseBuf++;
				break;
			case '*': // assert *2
				m_parseState = REDIS_PARSE_ARRAYLENGTH;
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_ARRAY;
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
				arrayInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;
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
//	std::stringstream log_msg;
//	log_msg << "connection start to parse redis response:[" << parseBuf << "], parseLen: " << parseLen;
//	LOG_WRITE_INFO(log_msg.str());

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
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_ERROR;
				break;
			case '+':
				m_parseState = REDIS_PARSE_RESULT;
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_STATUS;
				break;
			case ':':
				m_parseState = REDIS_PARSE_INTEGER;
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_INTEGER;
				break;
			case '$':
				m_parseState = REDIS_PARSE_LENGTH;
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;
				m_arrayNum = 1;
				break;
			case '*':
				m_parseState = REDIS_PARSE_ARRAYLENGTH;
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_ARRAY;
				break;
			default:
				replyInfo.replyType = RedisReplyType::REDIS_REPLY_UNKNOWN;
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
					arrayInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;
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
					arrayInfo.replyType = RedisReplyType::REDIS_REPLY_NIL;
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
				arrayInfo.replyType = RedisReplyType::REDIS_REPLY_STRING;
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
