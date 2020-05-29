#include"util.h"
#include"redisbase.h"

#include<climits>

namespace GBDownLinker {

int string2ll_norah(char* s, std::size_t slen, long long* value)
{
	if (slen == 1 && s[0] == '0')
	{
		if (value != NULL)
			*value = 0;
		return REDIS_OK;
	}

	char* e = s + slen;
	char** end = &e;
	long long v = strtoll(s, end, 10);
	if (v == 0) // 不能进行转换时，返回0
	{
		return REDIS_ERR;
	}
	else if (v == LLONG_MIN || v == LLONG_MAX) // 溢出时，返回
	{
		return REDIS_ERR;
	}

	if (value != NULL)
		*value = v;
	return REDIS_OK;
}

int string2ll(char* s, std::size_t slen, long long* value)
{
//    LOG_DEBUG("string2ll slen: %zd\n", slen);
//    printf("[");
//    for(std::size_t i=0; i<slen; ++i)
//    {
//        printf("%c", s[i]);
//    }
//    printf("]\n");

	int negative = 0;
	char* p = s;
	std::size_t plen = 0;
	unsigned long long v = 0;

	if (slen == 0)
		return REDIS_ERR;
	if (slen == 1 && p[0] == '0')
	{
		if (value != NULL)
			*value = 0;
		return REDIS_OK;
	}

	if (p[0] == '-')
	{
		negative = 1;
		p++;
		plen++;
		if (plen == slen)
			return REDIS_ERR;
	}

	if (p[0] >= '1'  &&  p[0] <= '9')
	{
		v = p[0] - '0';
		p++;
		plen++;
	}
	else if (p[0] == '0' && slen == 1)
	{
		*value = 0;
		return REDIS_OK;
	}
	else
	{
		return REDIS_ERR;
	}

	while (plen < slen && p[0] >= '0' && p[0] <= '9')
	{
		if (v > (ULONG_MAX / 10))
			return REDIS_ERR;
		v *= 10;

		if (v > (ULONG_MAX - (p[0] - '0')))
			return REDIS_ERR;
		v += p[0] - '0';
		p++;
		plen++;
	}

	if (plen < slen)
		return REDIS_ERR;

	if (negative)
	{
		if(v>((unsigned long long)(-(LLONG_MIN+1))+1)) // if(v > ((unsigned long long)LLONG_MAX）+1)
			return REDIS_ERR;
		if(value!=NULL)
			*value=-v;
	}
	else
	{
		if (v > LLONG_MAX)
			return REDIS_ERR;
		if (value != NULL)
			*value = v;
	}
	return REDIS_OK;
}

}
