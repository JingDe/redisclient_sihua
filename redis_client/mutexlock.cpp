#include "mutexlock.h"

namespace GBDownLinker {

MutexLock::MutexLock()
{
	pthread_mutex_init(&mutex_, NULL);
}

MutexLock::~MutexLock()
{
	pthread_mutex_destroy(&mutex_);
}

void MutexLock::Lock()
{
	pthread_mutex_lock(&mutex_);
}

void MutexLock::Unlock()
{
	pthread_mutex_unlock(&mutex_);
}

} // namespace GBDownLinker

