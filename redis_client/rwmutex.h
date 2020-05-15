#ifndef DOWN_DATA_RESTORER_RWMUTEX_RWLOCK_H_
#define DOWN_DATA_RESTORER_RWMUTEX_RWLOCK_H_

#include <cstdint>
#include <pthread.h>
#include <sys/types.h>

namespace GBDownLinker {

class RWMutex
{
public:
	RWMutex();
	virtual ~RWMutex();

	int acquireRead(void);
	int acquireWrite(void);

	void acquireCertainWrite(int num);
	void releaseCertain(void);

	void release(void);

protected:

	int32_t				m_refCount;
	int32_t				m_numWaitingWriters;
	int32_t				m_numWaitingReads;
	pthread_mutex_t 	m_rwLock;
	pthread_cond_t		m_waitingWriters;
	pthread_cond_t		m_waitingReads;

	pthread_rwlock_t   m_rwlock;
};

class ReadGuard
{
public:
	ReadGuard(RWMutex& rwMutex);
	virtual ~ReadGuard();

private:
	RWMutex* m_rwMutex;
	int 			m_ret;
};

class WriteGuard
{
public:
	WriteGuard(RWMutex& rwMutex);
	virtual ~WriteGuard();

private:
	RWMutex* m_rwMutex;
	int 			m_ret;
};

class CertainWriteGuard
{
public:
	CertainWriteGuard(RWMutex& rwMutex, int num);
	virtual ~CertainWriteGuard();

private:
	RWMutex* m_rwMutex;
};

} // namespace GBDownLinker

#endif

