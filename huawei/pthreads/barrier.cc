#include "barrier.h"
#include "errors.h"
#include <pthread.h>

/*
 * Initialize a barrier for use.
 */
int barrier_init(barrier_t * barrier, int count)
{
	int status;

	barrier->threshold = barrier->counter = count;
	barrier->cycle = 0;
	status = pthread_mutex_init(&barrier->mutex, NULL);
	if (status != 0) {
		return status;
	}
	status = pthread_cond_init(&barrier->cv, NULL);
	if (status != 0) {
		pthread_mutex_destroy(&barrier->mutex);
		return status;
	}
	barrier->valid = BARRIER_VALID;

	return 0;
}

/*
 * Destroy a barrier when done use it.
 */
int barrier_destroy(barrier_t * barrier)
{
	int status, status2;

	if (barrier->valid != BARRIER_VALID) {
		return EINVAL;
	}

	/* Set barrier invalid. */
	status = pthread_mutex_lock(&barrier->mutex);
	if (status != 0) {
		return status;
	}
	if (barrier->counter != barrier->threshold) {
		pthread_mutex_unlock(&barrier->mutex);
		return EBUSY;
	}
	barrier->valid = 0;
	status = pthread_mutex_unlock(&barrier->mutex);
	if (status != 0) {
		return status;
	}
	
	status = pthread_mutex_destroy(&barrier->mutex);
	status2 = pthread_cond_destroy(&barrier->cv);
	return (status != 0 ? status : status2);
}

/*
 * Wait all threads reached.
 */
int barrier_wait(barrier_t * barrier)
{
	int status, cycle, cancel, tmp;
	if (barrier->valid != BARRIER_VALID) {
                return EINVAL;
        }

	status = pthread_mutex_lock(&barrier->mutex);
	if (status != 0) {
		return status;
	}

	cycle = barrier->cycle;

	/* If the last thread arrived, wake others */
	if (--barrier->counter == 0) {
		barrier->counter = barrier->threshold;
		barrier->cycle ++;
		status = pthread_cond_broadcast(&barrier->cv);

		/* The last thread return -1 rather than 0, so that
		 * it can be used to do some special serial code following
		 * the barrier.
		 */
		if (status == 0) {
			status = -1;
		}
	} else {
		/* Wait with cancellation disabled, because barrier_wait
		 * should not be a cancellation point.
		 */
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &cancel);

		while (cycle == barrier->cycle) {
			status = pthread_cond_wait(&barrier->cv, &barrier->mutex);
			if (status != 0) {
				break;
			}
		}
		pthread_setcancelstate(cancel, &tmp);
	}
	
	pthread_mutex_unlock(&barrier->mutex);
	return status;
}
