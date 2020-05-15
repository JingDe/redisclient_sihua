#ifndef DOWN_DATA_RESTORER_MUTEX_LOCK_GUARD_H
#define DOWN_DATA_RESTORER_MUTEX_LOCK_GUARD_H

#include "mutexlock.h"

namespace GBDownLinker {

class MutexLockGuard {
public:
	explicit MutexLockGuard(MutexLock* lock);

	~MutexLockGuard();

private:
	MutexLock* mutex_lock_;

	MutexLockGuard(const MutexLockGuard&);
	MutexLockGuard& operator=(const MutexLockGuard&);
};

} // namespace GBDownLinker

#endif
