#ifndef DOWN_DATA_RESTORER_DBSTREAM_H
#define DOWN_DATA_RESTORER_DBSTREAM_H

#include <stdint.h>
#include <string>
#include <list>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include <bitset>
#include <string.h>


using namespace std;

namespace GBDownLinker {

typedef list<string> StrList;

#define MAX_BLOCK_SIZE 4096

class DBOutStream
{
public:
	DBOutStream()
	{
		m_capacity = MAX_BLOCK_SIZE;
		m_data = new char[m_capacity];
		memset(m_data, 0, m_capacity);
		m_cursor = m_data;
	}
	virtual ~DBOutStream() {
		delete[] m_data;
	}

	char* getData() {
		return m_data;
	}
	int getSize() {
		return (m_cursor - m_data);
	}

	inline void save(void* p, int size);

	DBOutStream& operator <<(bool i);
	DBOutStream& operator <<(char i);
	//	DBOutStream &operator <<(unsigned char i);
	DBOutStream& operator <<(unsigned short i);
	DBOutStream& operator <<(uint64_t i);
	DBOutStream& operator <<(uint32_t i);
	DBOutStream& operator <<(int8_t i);
	DBOutStream& operator <<(uint8_t i);
	DBOutStream& operator <<(int16_t i);
	DBOutStream& operator <<(int32_t i);
	DBOutStream& operator <<(int64_t i);
	DBOutStream& operator <<(float f);

	DBOutStream& operator <<(const char* str);
	DBOutStream& operator <<(const string& str);
	DBOutStream& operator <<(const StrList& strList);

private:
	//char m_data[MAX_BLOCK_SIZE];
	char* m_data;
	char* m_cursor;
	int m_capacity;
};


class DBInStream
{
public:
	DBInStream(void* d, int n)
	{
		m_cursor = m_data = (char*)d;
		m_datalen = n;
		m_loadError = 0;
	}
	virtual  ~DBInStream() {}
	inline void load(void* p, int size);

	DBInStream& operator >>(bool& i);
	DBInStream& operator >>(char& i);
	//	DBInStream &operator >>(unsigned char &i);
	DBInStream& operator >>(unsigned short& i);
	DBInStream& operator >>(uint64_t& i);
	DBInStream& operator >>(uint32_t& i);
	DBInStream& operator >>(int8_t& i);
	DBInStream& operator >>(uint8_t& i);
	DBInStream& operator >>(int16_t& i);
	DBInStream& operator >>(int32_t& i);
	DBInStream& operator >>(int64_t& i);
	DBInStream& operator >>(float& f);

	DBInStream& operator >>(char*& str);
	DBInStream& operator >>(string& str);
	DBInStream& operator >>(StrList& strList);

public:
	int m_loadError;

private:
	char* m_data;
	char* m_cursor;
	int m_datalen;
};

} // namespace GBDownLinker

#endif
