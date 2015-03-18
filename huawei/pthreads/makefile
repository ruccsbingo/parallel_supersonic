rwlock_main.app:rwlock_main.c rwlock.c
	gcc -o rwlock_main.app -lpthread -lrt rwlock_main.c rwlock.c

barrier_main.app:barrier_main.c barrier.c
	gcc -o barrier_main.app -lpthread -lrt barrier_main.c barrier.c

susp.app:susp.c
	gcc -o susp.app -lpthread -lrt susp.c


clean:
	rm *.o *.app
