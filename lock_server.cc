// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0),
  mutex(PTHREAD_MUTEX_INITIALIZER),
  cond(std::map<lock_protocol::lockid_t, pthread_cond_t>()),
  lock_stat(std::map<lock_protocol::lockid_t, int>())
{
}


lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here

  pthread_mutex_lock(&mutex);
  if (cond.find(lid) == cond.end()) {
    cond[lid] = PTHREAD_COND_INITIALIZER;
  } else {
    while(lock_stat[lid]) 
      pthread_cond_wait (&(cond[lid]), &mutex );
  }

  lock_stat[lid] = 1;
  nacquire++;
  r = nacquire;
  pthread_mutex_unlock(&mutex);

  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  printf("release lock[%d]\n", lid);
  pthread_mutex_lock(&mutex);

  lock_stat[lid] = 0;

  pthread_cond_signal (&(cond[lid]));
  r = nacquire;
  pthread_mutex_unlock(&mutex);

  return ret;
}
