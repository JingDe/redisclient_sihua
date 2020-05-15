#include "dbstream.h"
#include "base_library/log.h"
#include <sstream>

namespace GBDownLinker {

void DBOutStream::save(void* p, int size)
{
	//check capacity: if not enough, extend the buffer
	while (m_cursor + size > m_data + m_capacity) {
		m_capacity = m_capacity * 2;
		char* newBuf = new char[m_capacity];
		if (newBuf == NULL) {
			std::stringstream log_msg;
			log_msg << "failed to new memory in DBOutStream::checkCapacity";
			LOG_WRITE_ERROR(log_msg.str());
			return;
		}
		memset(newBuf, 0, m_capacity);
		if (getSize() > 0) {
			memcpy(newBuf, m_data, getSize());
		}
		m_cursor = newBuf + getSize();
		delete[] m_data;
		m_data = newBuf;
	}

	memcpy(m_cursor, p, size);
	m_cursor += size;
}

DBOutStream& DBOutStream::operator <<(bool i)
{
	save((void*)&i, sizeof(i));
	return (*this);
}

DBOutStream& DBOutStream::operator <<(char i)
{
	save((void*)&i, sizeof(i));
	return (*this);
}

//DBOutStream &DBOutStream::operator <<(unsigned char i)
//{
//	save((void *)&i, sizeof(i));
//	return (*this);
//}

DBOutStream& DBOutStream::operator <<(unsigned short i)
{
	save((void*)&i, sizeof(i));
	return (*this);
}

DBOutStream& DBOutStream::operator <<(uint64_t i)
{
	save((void*)&i, sizeof(i));
	return (*this);
}

DBOutStream& DBOutStream::operator <<(uint32_t i)
{
	save((void*)&i, sizeof(i));
	return (*this);
}

DBOutStream& DBOutStream::operator <<(int8_t i)
{
	save((void*)&i, sizeof(i));
	return (*this);
}

DBOutStream& DBOutStream::operator <<(uint8_t i)
{
	save((void*)&i, sizeof(i));
	return (*this);
}

DBOutStream& DBOutStream::operator <<(int16_t i)
{
	save((void*)&i, sizeof(i));
	return (*this);
}

DBOutStream& DBOutStream::operator <<(int32_t i)
{
	/* origin
	if (m_cursor <= m_data + MAX_BLOCK_SIZE - sizeof(i)) {
		*(int32_t *) m_cursor = i;
		m_cursor += sizeof(i);
	}*/
	save((void*)&i, sizeof(i));
	return (*this);
}

DBOutStream& DBOutStream::operator <<(int64_t i)
{
	save((void*)&i, sizeof(i));
	return (*this);
}

DBOutStream& DBOutStream::operator <<(float f)
{
	save((void*)&f, sizeof(f));
	return (*this);
}

DBOutStream& DBOutStream::operator <<(const char* str)
{
	save((void*)str, strlen(str) + 1);
	return (*this);
}

DBOutStream& DBOutStream::operator <<(const string& str) {
	return operator <<(str.c_str());
}

DBOutStream& DBOutStream::operator <<(const StrList& strList)
{
	unsigned short n = strList.size();
	operator << (n);

	StrList::const_iterator it;
	for (it = strList.begin(); it != strList.end(); it++) {
		operator << (*it);
	}
	return (*this);
}


//==========================================================

void DBInStream::load(void* p, int size)
{
	if (m_cursor + size > m_data + m_datalen) {
		std::stringstream log_msg;
		log_msg << "can't load data from db! Did developer change db fields format?";
		LOG_WRITE_ERROR(log_msg.str());
		m_loadError = 1;
		return;
	}
	memcpy(p, m_cursor, size);
	m_cursor += size;
}

DBInStream& DBInStream::operator >>(bool& i)
{
	load((void*)&i, sizeof(i));
	return (*this);
}

DBInStream& DBInStream::operator >>(char& i)
{
	load((void*)&i, sizeof(i));
	return (*this);
}

//DBInStream &DBInStream::operator >>(unsigned char &i)
//{
//	load( (void *)&i, sizeof(i) );
//	return (*this);
//}

DBInStream& DBInStream::operator >>(unsigned short& i)
{
	load((void*)&i, sizeof(i));
	return (*this);
}

DBInStream& DBInStream::operator >>(uint64_t& i)
{
	load((void*)&i, sizeof(i));
	return (*this);
}

DBInStream& DBInStream::operator >>(uint32_t& i)
{
	load((void*)&i, sizeof(i));
	return (*this);
}

DBInStream& DBInStream::operator >>(int8_t& i)
{
	load((void*)&i, sizeof(i));
	return (*this);
}

DBInStream& DBInStream::operator >>(uint8_t& i)
{
	load((void*)&i, sizeof(i));
	return (*this);
}

DBInStream& DBInStream::operator >>(int16_t& i)
{
	load((void*)&i, sizeof(i));
	return (*this);
}

DBInStream& DBInStream::operator >>(int32_t& i)
{
	/* origin
	if (m_cursor <= m_data + m_datalen - sizeof(i)) {
		i = *(int32_t *) m_cursor;
		m_cursor += sizeof(i);
	} else
		i = 0;
	*/
	load((void*)&i, sizeof(i));
	return (*this);
}

DBInStream& DBInStream::operator >>(int64_t& i)
{
	load((void*)&i, sizeof(int64_t));
	return (*this);
}

DBInStream& DBInStream::operator >>(float& f)
{
	load((void*)&f, sizeof(float));
	return (*this);
}

DBInStream& DBInStream::operator >>(char*& str)
{
	//unsigned short len;
	//operator >>(len);

	//if (m_cursor <= m_data + m_datalen - len && !m_cursor[len - 1]) {
	if (1) {
		strcpy(str, m_cursor);
		//m_cursor += len;
		m_cursor += strlen(str) + 1;
	}
	else {
		char retStr[2] = "";
		str = retStr;
	}
	return (*this);
}

DBInStream& DBInStream::operator >>(string& str)
{
	str = m_cursor;
	m_cursor += str.length() + 1;
	return (*this);
}

DBInStream& DBInStream::operator >>(StrList& strList)
{
	unsigned short num;
	operator >>(num);

	strList.clear();
	while (num-- > 0) {
		string s;
		operator >>(s);
		strList.push_back(s);
	}
	return (*this);
}

} // namespace GBDownLinker
