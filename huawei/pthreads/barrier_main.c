#include <pthread.h>
#include "errors.h"
#include "barrier.h"

#define THREADS 5
#define ARRAY   6
#define INLOOPS 1000
#define OUTLOOPS 10


typedef struct thread_tag {
	pthread_t thread_id;
	int number;
	int increment;
	int array[ARRAY];
} thread_t;

barrier_t barrier;
thread_t threads[THREADS];

void * thread_routine(void * arg)
{
	thread_t * self = (thread_t *)arg;
	int in_loop, out_loop, count, status;

	for (out_loop = 0; out_loop < OUTLOOPS; out_loop ++) {
		status = barrier_wait(&barrier);
		if (status > 0) {
			err_abort(status, "Wait on barrier");
		}

		for (in_loop = 0; in_loop < INLOOPS; in_loop++) {
			for (count = 0; count < ARRAY; count++) {
				self->array[count] += self->increment;
			}
		}
		status = barrier_wait(&barrier);
		if (status > 0) {
			err_abort(status, "Wait on barrier");
		}

		if (status == -1) {
			int thread_num;
			for (thread_num = 0; thread_num < THREADS; thread_num ++) {
				threads[thread_num].increment += 1;
			}
		}
	}

	return NULL;
}


int main(int argc, char * argv[])
{
	int i, array_count;
	int status;

	barrier_init(&barrier, THREADS);

	for (i = 0; i < THREADS; i ++){
		status = pthread_create(&threads[i].thread_id, 0, thread_routine, &threads[i]);
		if (status != 0) {
			err_abort(status, "create thread");
		}
		threads[i].increment = i;
		threads[i].number = i;

		for (array_count = 0; array_count < ARRAY; array_count ++) {
			threads[i].array[array_count] = array_count + 1;
		}
	}

	for (i = 0; i < THREADS; i++) {
		status = pthread_join(threads[i].thread_id, NULL);
		if (status != 0) {
			err_abort(status, "Join thread");
		}
		printf("%02d : (%d) ", i, threads[i].increment);

		for (array_count = 0; array_count < ARRAY; array_count++) {
			printf ("%010u ", threads[i].array[array_count]); 
		}
		printf("\n");
	}

	barrier_destroy(&barrier);
	return 0;
}
