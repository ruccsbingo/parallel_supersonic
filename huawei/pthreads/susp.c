#include "errors.h"
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>


#define THREAD_COUNT    20
#define ITERATIONS      40000

unsigned long	thread_count	= THREAD_COUNT;
unsigned long	iterations	= ITERATIONS;
pthread_mutex_t	the_mutex	= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t	mut		= PTHREAD_MUTEX_INITIALIZER;
volatile sig_atomic_t sentinel	= 0; 
pthread_once_t once		= PTHREAD_ONCE_INIT;
pthread_t *array = NULL, null_pthread = {0};
int bottom	= 0;
int inited	= 0;

/*
 * Handle SIGUSER1 in the target thread, to suspend it until 
 * received SIGUSER2(resume).
 */
void suspend_signal_handler(int sig)
{
	sigset_t signal_set;
	
	/* Block all signals except SIGUSER2 while suspended. */
	sigfillset (&signal_set);
	sigdelset (&signal_set, SIGUSR2);
	sentinel	= 1;
	sigsuspend (&signal_set);

	/* Once i'm here, i've been resumed, and the resume signal
	 * handler has been run to completion.
	 */
	return;
}


/*
 * Handle SIGUSER2 in the target thread, to resume it. Note that
 * the signal handler does nothing. It exists only because we need
 * to cause sigsuspend() to return.
 */
void resume_signal_handler (int sig)
{
	return;
}

/* 
 * Dynamically initialize the "suspend package" when first use
 * (called by pthread_once).
 */
void suspend_init_routine (void)
{
	int status;
	struct sigaction sigusr1, sigusr2;

	/* Allocate the suspended threads array. This array is used
	 * to guarantee idempotency. 
	 */
	bottom	= 10;
	array	= (pthread_t *) calloc(bottom, sizeof(pthread_t));

	/* Install the signal handlers for suspend/resume. */
	sigusr1.sa_flags	= 0;
	sigusr1.sa_handler	= suspend_signal_handler;
	sigemptyset (&sigusr1.sa_mask);
	sigaddset (&sigusr1.sa_mask, SIGUSR2);
	sigusr2.sa_flags	= 0;
	sigusr2.sa_handler	= resume_signal_handler;
	sigemptyset (&sigusr2.sa_mask);

	status	= sigaction (SIGUSR1, &sigusr1, NULL);
	if (status == -1) {
		errno_abort("Installing suspend handler.");
	}

	status	= sigaction (SIGUSR2, &sigusr2, NULL);
	if (status == -1) {
		errno_abort("Installing resume handler.");
	}

	inited	= 1;
	return;
}


/* 
 * Suspend a thread by sending it a signal (SIGUSR1), which will block
 * the thread until another signal (SIGUSR2) arrives.
 *
 * Multiple calls to thd_suspend for a single thread have no additional
 * effect on the thread -- a single thd_continue call will cause it to
 * resume execution.
 */
int thd_suspend(pthread_t target_thread)
{
	int status;
	int i = 0;

	/* The first call to thd_suspend will initialize the package. */
	status	= pthread_once(&once, suspend_init_routine);
	if (status != 0) {
		return status;
	}

	status = pthread_mutex_lock(&mut);
	if (status != 0 ) { 
		return status;
	}

	/*
	 * Threads that are suspended are added to the target_array;
	 * a request to suspend a thread already listed in the array is
	 * ignored. Sending a second SIGUSR1 would cause the thread to
	 * resuspend itself as soon as it is resumed.
	 */
	while (i < bottom) {
		if (pthread_equal (array[i++], target_thread)) {
			status = pthread_mutex_unlock(&mut);
			return status;
		}
	}

	i	= 0;
	while ( i < bottom && array[i] != 0) {
		i ++;
	}

	if ( i == bottom) {
		array = (pthread_t *) realloc (array, (++bottom * sizeof(pthread_t)));
		if (array == NULL) {
			pthread_mutex_unlock(&mut);
			return errno;
		}
		array[bottom] = null_pthread;
	}

	/* Clear the sentinel and signal the thread to suspend. */
	sentinel	= 0;
	status		= pthread_kill(target_thread, SIGUSR1);
	if ( status != 0 ) {
		pthread_mutex_unlock(&mut);
		return status;
	}

	/* Wait for the sentinel to change. */
	while (sentinel == 0) {
		sched_yield();
	}

	array[i]	= target_thread;
	status	= pthread_mutex_unlock(&mut);
	return status;
}

/* 
 * Resume a suspended thread by sending it SIGUSR2 to break it out of 
 * the sigsuspend() in which it's waiting. If the target thread isn't
 * suspended, return with success.
 */
int thd_continue(pthread_t target_thread)
{
	int status;
	int i = 0;
	status	= pthread_once(&once, suspend_init_routine);
	if (status != 0) {
		return status;
	}

	status = pthread_mutex_lock(&mut);
	if (status != 0 ) { 
		return status;
	}

	while (i < bottom && !pthread_equal(array[i], target_thread)) {
		i++;
	}
	
	if (i == bottom) {
		pthread_mutex_unlock(&mut);
		return 0;
	}

	/* Signal the thread to continue, and remove the thread from the 
	 * suspended array.
	 */
	status = pthread_kill(target_thread, SIGUSR2);
	if (status != 0) {
		pthread_mutex_unlock(&mut);
		return status;
	}
	
	array[i] = 0;
	status = pthread_mutex_unlock(&mut);
	return status;
}


static void * thread_routine(void * arg)
{
	int number = (int ) arg;
	int status;
	int i;
	char buffer[128];

	for( i = 1; i <= iterations; i++) {
		if( i % 2000 == 0) {
			sprintf(buffer, "Thread %02d: %d\n", number, i);
			write ( 1, buffer, strlen(buffer));
		}

		sched_yield();
	}

	return (void *) 0;
}

int main(int argc, char * argv[])
{
	pthread_t threads[THREAD_COUNT];
	pthread_attr_t detach;
	int status;
	void * result;
	int i;

	status = pthread_attr_init(&detach);
	if (status != 0) {
		err_abort(status, "Init attributes object");
	}

	status = pthread_attr_setdetachstate(&detach, PTHREAD_CREATE_DETACHED);
	if (status != 0) {
		err_abort(status, "Set create-detached.");
	}

	for (i = 0; i < THREAD_COUNT; i++) {
		status = pthread_create(&threads[i], &detach, thread_routine, (void *) i);
		if (status != 0) {
			err_abort(status, "Create threads.");
		}
	}

	for( i = 0; i < THREAD_COUNT/2; i++) {
		printf("Suspending thread %d.\n", i);
		status = thd_suspend(threads[i]);
		if (status != 0) {
			err_abort(status, "Suspend threads.");
		}
	}

	printf ("Sleeping ... \n");
	sleep(2);

	for( i = 0; i < THREAD_COUNT/2; i++) {
		printf("Resume thread %d.\n", i);
		status = thd_continue(threads[i]);
		if (status != 0) {
			err_abort(status, "Resume threads.");
		}
	}

	for( i = THREAD_COUNT/2; i < THREAD_COUNT; i++) {
		printf("Suspending thread %d.\n", i);
		status = thd_suspend(threads[i]);
		if (status != 0) {
			err_abort(status, "Suspend threads.");
		}
	}

	printf ("Sleeping ... \n");
	sleep(2);

	for( i = THREAD_COUNT/2; i < THREAD_COUNT; i++) {
		printf("Resume thread %d.\n", i);
		status = thd_continue(threads[i]);
		if (status != 0) {
			err_abort(status, "Resume threads.");
		}
	}

	pthread_exit(NULL);
}
	

