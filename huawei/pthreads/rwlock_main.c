#include "rwlock.h"
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include "errors.h"

#define THREADS		5
#define DATASIZE	15
#define ITERATIONS	10000

/*
 *Keep statistics for each thread.
 */
typedef struct thread_tag{
	int		thread_num;
	pthread_t	thread_id;
	int		updates;
	int		reads;
	int		interval;
}thread_t;

/*
 * Read/write lock and shared data.
 */
typedef struct data_tag{
	rwlock_t	lock;
	int		data;
	int		updates;
} data_t;

thread_t threads[THREADS];
data_t	data[DATASIZE];


void * thread_routine(void * arg)
{
	thread_t * self = (thread_t *)arg;
	int repeats = 0;
	int iteration;
	int element = 0;
	int status;

	for (iteration = 0; iteration < ITERATIONS; iteration++){
		/* Each "self->interval" iterations, perfor an update
		 * operation(write lock instead of read lock).
		 */

		if (iteration % self->interval == 0) {
			status = rwlock_writelock(&data[element].lock);
			if (status != 0) {
				err_abort(status, "write lock");
			}

			data[element].data = self->thread_num;
			data[element].updates ++;
			self->updates++;
			status = rwlock_writeunlock(&data[element].lock);
			if (status != 0) {
				err_abort(status, "write unlock");
			}
		} else {
			/* Look at the current data element to see whether
			 * the current thread last update it. Count the times,
			 * to report later. 
			 */
			status = rwlock_readlock(&data[element].lock);
			if (status != 0) {
				err_abort(status, "read lock");
			}
			self->reads ++;
			if(data[element].data == self->thread_num){
				repeats ++;
			}
			status = rwlock_readunlock(&data[element].lock);
			if (status != 0) {
				err_abort(status, "read unlock");
			}
		}

		element++;
		if (element >= DATASIZE){
			element = 0;
		}
	}

	if(repeats > 0) {
		printf("Thread %d found unchanged elements %d times\n",
			self->thread_num, repeats);
	}

	return NULL;
}

int main(int argc, char * argv[])
{

	int i;
	int status;
	unsigned int seed = 1;
	int thread_updates = 0;
	int data_updates = 0;

	for (i = 0; i < DATASIZE; i++){
		data[i].data = 0;
		data[i].updates = 0;
		status = rwlock_init(&data[i].lock);
		if (status != 0) {
			err_abort(status, "Init rw lock");
		}
	}

	for(i = 0; i < THREADS; i++) {
		threads[i].thread_num	= i;
		threads[i].updates	= 0;
		threads[i].reads	= 0;
		threads[i].interval	= rand_r(&seed)%71;
		status = pthread_create(&threads[i].thread_id,
			NULL, thread_routine, &threads[i]);
		if (status != 0) {
			err_abort(status, "Create thread");
		}
	}
		
	for(i = 0; i < THREADS; i++) {
		status = pthread_join(threads[i].thread_id,NULL);
		if (status != 0) {
			err_abort(status, "join thread");
		}
		thread_updates += threads[i].updates;
		printf("%2d: interval %d, updates %d, reads %d\n", i, threads[i].interval,
			threads[i].updates, threads[i].reads);
	}

	for(i = 0; i < DATASIZE; i++) {
		data_updates += data[i].updates;
		printf("data %2d: value %d, %d updates \n", i, data[i].data,
				data[i].updates);
		rwlock_destroy(&data[i].lock);
	}

	printf("%d thread updates, %d data updates\n",
		thread_updates, data_updates);
	return 0;
}
