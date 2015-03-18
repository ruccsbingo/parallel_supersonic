#ifndef __RWLOCK_HH__
#define __RWLOCK_HH__

#include <pthread.h>

#define RWLOCK_VALID   0xabcdef

typedef struct rwlock_tag{
	pthread_mutex_t mutex;     /* Access locker    */
	pthread_cond_t  read;      /* Wait for read    */
	pthread_cond_t  write;     /* Wait for write   */
	int             r_wait;    /* Waiting readers  */
	int             w_wait;    /* Waiting writers  */
        int             r_active;  /* Activing readers */
        int             w_active;  /* Activing writers */
        int             valid;     /* Set when valid   */
}rwlock_t;

/*
 * Static initializer.
 */
#define RWLOCK_INITIALIZER \
	{PTHREAD_MUTEX_INITIALIZER,PTHREAD_COND_INITIALIZER,\
	 PTHREAD_COND_INITIALIZER, 0,0,0,0,RWLOCK_VALID} 

extern int rwlock_init(rwlock_t * rwlock);
extern int rwlock_destroy(rwlock_t * rwlock);
extern int rwlock_readlock(rwlock_t * rwlock);
extern int rwlock_tryreadlock(rwlock_t * rwlock);
extern int rwlock_writelock(rwlock_t * rwlock);
extern int rwlock_trywritelock(rwlock_t * rwlock);
extern int rwlock_readunlock(rwlock_t * rwlock);
extern int rwlock_writeunlock(rwlock_t * rwlock);

#endif

