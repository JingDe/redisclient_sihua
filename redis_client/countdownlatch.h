#ifndef DOWN_DATA_RESTORER_COUNTDOWN_LATCH_H
#define DOWN_DATA_RESTORER_COUNTDOWN_LATCH_H

#include <pthread.h>

namespace GBDownLinker {

class CountDownLatch {
public:
	explicit CountDownLatch(int count);
	~CountDownLatch();
	void Wait();
	void CountDown();
	bool IsValid() { return valid_; }

private:
	pthread_mutex_t mutex_;
	pthread_cond_t condition_;
	int count_;
	bool valid_;
	bool mutex_inited_;
	bool cond_inited_;
};

} // namespace GBDownLinker

#endif
