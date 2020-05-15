#include "condvar.h"
#include <time.h>
#include <cstdint>

namespace GBDownLinker {

CondVar::CondVar(MutexLock* mu) :mu_(mu)
{
	pthread_cond_init(&cv_, NULL);
}

CondVar::~CondVar()
{
	pthread_cond_destroy(&cv_);
}

void CondVar::Wait()
{
	pthread_cond_wait(&cv_, &mu_->mutex_);
}

void CondVar::WaitForSeconds(int secs)
{
	struct timespec abstime;
	clock_gettime(CLOCK_REALTIME, &abstime); // 获得特定clock的时间

	const int64_t kNanoSecondsPerSecond = 1e9;
	int64_t nanoseconds = static_cast<int64_t>(secs * kNanoSecondsPerSecond);

	abstime.tv_sec += static_cast<time_t>((abstime.tv_nsec + nanoseconds) / kNanoSecondsPerSecond);
	abstime.tv_nsec = static_cast<long>((abstime.tv_nsec + nanoseconds) % kNanoSecondsPerSecond);

	pthread_cond_timedwait(&cv_, &mu_->mutex_, &abstime); // 等待到一个绝对时间
}

void CondVar::Signal()
{
	pthread_cond_signal(&cv_);
}

void CondVar::SignalAll()
{
	pthread_cond_broadcast(&cv_);
}

} // namespace GBDownLinker
