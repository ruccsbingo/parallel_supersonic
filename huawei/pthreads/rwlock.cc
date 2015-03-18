#include "rwlock.h"
#include <pthread.h>
#include <errno.h>

int rwlock_init(rwlock_t * rwlock)
{
	int status;
	status = pthread_mutex_init(&rwlock->mutex, NULL);
	if (status != 0){
		return status;
	}	
	status = pthread_cond_init(&rwlock->read, NULL);
	if (status != 0) {
		pthread_mutex_destroy(&rwlock->mutex);
		return status;
	}
	status = pthread_cond_init(&rwlock->write, NULL);
	if (status != 0) {
		pthread_mutex_destroy(&rwlock->mutex);
		pthread_cond_destroy(&rwlock->read);
		return status;
	}

	rwlock->r_wait = rwlock->r_active = rwlock->w_wait = rwlock->w_active = 0;
	rwlock->valid = RWLOCK_VALID;
	return 0;
}


int rwlock_destroy(rwlock_t * rwlock)
{
	int status, status1, status2;
	if (rwlock->valid != RWLOCK_VALID) {
		return EINVAL;
	}

	status = pthread_mutex_lock(&rwlock->mutex);
	if (status != 0) {
		return status;
	}

	if ( rwlock->r_wait > 0 ||rwlock-> w_wait > 0
		 || rwlock->r_active > 0 || rwlock->w_active > 0) {
		pthread_mutex_unlock(&rwlock->mutex);
		return EBUSY;
	}

	rwlock->valid = 0;
	status = pthread_mutex_unlock(&rwlock->mutex);
	if (status != 0) { 
		return status;
	}

	status  = pthread_mutex_destroy(&rwlock->mutex);
	status1 = pthread_cond_destroy(&rwlock->read);
	status2 = pthread_cond_destroy(&rwlock->write);
	return status != 0 ? status : (status1 != 0 ? status1 : status2);
}


static void  rwlock_readcleanup(void * arg) 
{
	rwlock_t * rwlock = (rwlock_t *)arg;
	--rwlock->r_wait;
	pthread_mutex_unlock(&rwlock->mutex);
}


int rwlock_readlock(rwlock_t * rwlock)
{
	int status;

	if (rwlock->valid != RWLOCK_VALID) {
		return EINVAL;
	}

	status = pthread_mutex_lock(&rwlock->mutex);
	if (status != 0) {
		return status;
	}

	if (rwlock->w_active > 0) {
		rwlock->r_wait++;
		/* As read lock allow thread be canceled,
		 * set cleanup to release resource.
		 */
		pthread_cleanup_push(rwlock_readcleanup, (void *)rwlock);

		while (rwlock->w_active > 0) { 
			status = pthread_cond_wait(&rwlock->read,
					 &rwlock->mutex);
			if (status != 0) {
				break;
			}
		}
		pthread_cleanup_pop(0);
		rwlock->r_wait--;
	} 

	if (status == 0) {
		rwlock->r_active ++;
	}

	pthread_mutex_unlock(&rwlock->mutex);
	return status;
}


int rwlock_tryreadlock(rwlock_t * rwlock)
{
	int status;
	if (rwlock->valid != RWLOCK_VALID) {
                return EINVAL;
        }

	status = pthread_mutex_lock(&rwlock->mutex);
	if (status == 0) {
		if (rwlock->w_active == 0) {
			rwlock->r_active ++;
		}else {
			status = EBUSY;
		}
	}
	pthread_mutex_unlock(&rwlock->mutex);

	return status;
}


static void rwlock_writecleanup(void * arg)
{
	rwlock_t * rwlock = (rwlock_t *)arg;
	--rwlock->w_wait;
	pthread_mutex_unlock(&rwlock->mutex);
}


int rwlock_writelock(rwlock_t * rwlock) 
{
	int status;
	if (rwlock->valid != RWLOCK_VALID) {
		return EINVAL;
	}

	status = pthread_mutex_lock(&rwlock->mutex);
	if (status != 0) {
		return status;
	}

	if (rwlock->r_active > 0 || rwlock->w_active > 0 ) {
		rwlock->w_wait ++;
		pthread_cleanup_push(rwlock_writecleanup, rwlock);
		while(rwlock->r_active > 0 || rwlock->w_active > 0 ) {
			status = pthread_cond_wait(&rwlock->write,
					 &rwlock->mutex);
			if ( status != 0) {
				break;
			}
		}
		pthread_cleanup_pop(0);
		--rwlock->w_wait;
	}

	if ( status == 0) {
		rwlock->w_active = 1;
	}
	pthread_mutex_unlock(&rwlock->mutex);

	return status;
}


int rwlock_trywritelock(rwlock_t * rwlock)
{
	int status,status2;
	if (rwlock->valid != RWLOCK_VALID) {
		return EINVAL;
	}

	status = pthread_mutex_lock(&rwlock->mutex);
	if (status != 0) {
		return status;
	}	

	if (rwlock->r_active || rwlock->r_wait > 0 
		|| rwlock->w_active || rwlock->w_wait > 0) {
		status = EBUSY;	
	}
	else { 
		rwlock->w_active = 1;
	}

	status2 = pthread_mutex_lock(&rwlock->mutex);
	return status != 0? status:status2;
}

int rwlock_readunlock(rwlock_t * rwlock)
{
	int status, status2;
	if (rwlock->valid != RWLOCK_VALID) {
		return EINVAL;
	}

	status = pthread_mutex_unlock(&rwlock->mutex);
	if (status != 0) {
		return status;
	}

	if (--rwlock->r_active == 0) {
		if (rwlock->w_wait > 0){
			status = pthread_cond_signal(&rwlock->write);
		}
	}

	status2 = pthread_mutex_unlock(&rwlock->mutex);
	return status != 0 ? status : status2;
}


int rwlock_writeunlock(rwlock_t * rwlock)
{
	int status,status2;
	if (rwlock->valid != RWLOCK_VALID) {
		return EINVAL;
	}

	status = pthread_mutex_unlock(&rwlock->mutex);
	if (status != 0) {
		return status;
	}

	rwlock->w_active = 0;
	if (rwlock->r_wait > 0){
		status = pthread_cond_broadcast(&rwlock->read);
	}
	else if (rwlock->w_wait > 0) {
		status = pthread_cond_signal(&rwlock->write);
	}

	status2 = pthread_mutex_unlock(&rwlock->mutex);
	return status != 0 ? status : status2;
}

