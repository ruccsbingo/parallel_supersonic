/*
 * A barrier used to block all threads to reach a same point,
 * then they can goto next stage at the same time.
 */

#ifndef __BARRIER_H
#define __BARRIER_H
#include <pthread.h>

/*
 * Structure describing a barrier.
 */
typedef struct barrier_tag {
	pthread_mutex_t	mutex;	/* Control access to barrier */
	pthread_cond_t	cv;	/* Wait for barrier */
	int		valid;	/* Set when valid */
	int		threshold;	/* number of threads required */
	int		counter;	/* cuurent number of threads */
	unsigned long	cycle;		/* count cycles */
} barrier_t;

#define BARRIER_VALID	0xdbcafe

/*
 * Support static initialization of barriers.
 */
#define BARRIER_INITIALIZER(cnt) \
	{PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, \
	BARRIER_VALID, cnt, cnt, 0}

/*
 * Define barrier operations.
 */
extern int barrier_init(barrier_t * barrier, int count); /* dynamic initialization of barriers */
extern int barrier_destroy(barrier_t * barrier);         /* destroy the barrier */
extern int barrier_wait(barrier_t * barrier);            /* wait until the barrier is actived */

#endif
