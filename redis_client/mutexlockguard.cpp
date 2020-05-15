#include "mutexlockguard.h"

namespace GBDownLinker {

MutexLockGuard::MutexLockGuard(MutexLock* lock) {
	mutex_lock_ = lock;
	mutex_lock_->Lock();
}

MutexLockGuard::~MutexLockGuard() {
	mutex_lock_->Unlock();
}


} // namespace GBDownLinker

