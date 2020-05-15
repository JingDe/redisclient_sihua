#ifndef DOWN_DATA_RESTORER_CONDITION_VAR_H
#define DOWN_DATA_RESTORER_CONDITION_VAR_H

#include "mutexlock.h"
#include <pthread.h>

namespace GBDownLinker {

class CondVar {
public:
	explicit CondVar(MutexLock* mu);
	~CondVar();

	void Wait();
	void WaitForSeconds(int secs);
	void Signal();
	void SignalAll();

private:
	MutexLock* mu_;
	pthread_cond_t cv_;

	CondVar(const CondVar&);
	CondVar& operator=(const CondVar&);
};

} // namespace GBDownLinker

#endif
