#ifndef HIREDIS_UTIL_H
#define HIREDIS_UTIL_H

#include<cstddef>

namespace GBDownLinker {

int string2ll(char* s, std::size_t slen, long long* value);
int string2ll_norah(char* s, std::size_t slen, long long* value);

#ifndef NDEBUG
#define LOG_DEBUG(fmt, ...)  printf("[%s:%d:%s]  " fmt "\n", __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__)
#else
#define LOG_DEBUG(fmt, ...)
#endif

}

#endif
