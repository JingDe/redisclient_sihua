#ifndef DOWN_DATA_RESTORER_MUTEX_LOCK_H
#define DOWN_DATA_RESTORER_MUTEX_LOCK_H

#include <pthread.h>

namespace GBDownLinker {

class CondVar;

class MutexLock {
	friend class CondVar;
public:
	MutexLock();
	~MutexLock();
	void Lock();
	void Unlock();

	pthread_mutex_t* mutex() { return &mutex_; }

private:
	pthread_mutex_t mutex_;

	MutexLock(const MutexLock&);
	MutexLock& operator=(const MutexLock&);
};

} // namespace GBDownLinker {

#endif
