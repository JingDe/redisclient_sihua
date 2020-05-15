#ifndef REDIS_CLIENT_UTIL_H_
#define REDIS_CLIENT_UTIL_H_

#include <string>
#include <sstream>
#include <stdint.h>

using std::string;
using std::ostringstream;

namespace GBDownLinker {

template<class DataType>
inline string toStr(DataType data)
{
	if (data == DataType(-1))
		return "-1";

	ostringstream ostr;
	ostr << data;
	return ostr.str();
}


uint16_t crc16(const char* buf, int len);
time_t getCurrTimeSec();

} // namespace GBDownLinker

#endif
