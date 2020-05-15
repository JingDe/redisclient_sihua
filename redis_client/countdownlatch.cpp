#include "countdownlatch.h"
#include <cassert>

namespace GBDownLinker {

CountDownLatch::CountDownLatch(int count)
	:count_(count),
	valid_(false),
	mutex_inited_(false),
	cond_inited_(false)
{
	if (pthread_mutex_init(&mutex_, NULL) == 0)
	{
		mutex_inited_ = true;
	}
	else
	{
		return;
	}
	if (pthread_cond_init(&condition_, NULL) == 0)
	{
		cond_inited_ = true;
	}
	else
	{
		return;
	}
	valid_ = true;
}

CountDownLatch::~CountDownLatch()
{
	if (mutex_inited_)
		pthread_mutex_destroy(&mutex_);
	if (cond_inited_)
		pthread_cond_destroy(&condition_);
}


void CountDownLatch::Wait()
{
	assert(valid_);
	pthread_mutex_lock(&mutex_);
	while (count_)
		pthread_cond_wait(&condition_, &mutex_);
	pthread_mutex_unlock(&mutex_);
}

void CountDownLatch::CountDown()
{
	assert(valid_);
	pthread_mutex_lock(&mutex_);
	count_--;
	if (count_ == 0)
		pthread_cond_broadcast(&condition_);
	pthread_mutex_unlock(&mutex_);
}

}
