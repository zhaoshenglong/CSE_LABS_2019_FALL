// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

#define MAX_TIME_TO_REVOKE 5


int lock_client_cache::last_port = 0;

int Pthread_cond_init(pthread_cond_t cv) {
  int res = pthread_cond_init(&cv, NULL);
  VERIFY(res == 0);
  return res;
}

int Pthread_mutex_init(pthread_mutex_t mx) {
  int res = pthread_mutex_init(&mx, NULL);
  VERIFY(res == 0);
  return res;
}

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  Pthread_mutex_init(mutex);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  pthread_mutex_lock(&mutex);

  lock_client_cache::lock_entry *lock;
  if (locks.find(lid) == locks.end()){
    locks[lid] = new lock_entry();
  }
  lock = locks[lid];

  thread_entry *te = new thread_entry();
  te->id = pthread_self();
  Pthread_cond_init(te->cv);
  lock->threads.push_back(te);

  tprintf("[%d] Acquire lock[%llu], threads: %lu\n", rlock_port, lid, lock->threads.size());
  // whether there is someone waiting / holding the lock
  // RELEASING happened when last state is free and no threads waiting for it.
  // befire this acquire, server send a revoke
  if (lock->threads.size() > 1 || lock->state == RELEASING) {

    pthread_cond_wait(&te->cv, &mutex);
    tprintf("[%d] Acquire Next waked lid:%llu state: %d\n", rlock_port, lid, lock->state);
    if (lock->state == NONE) {
      lock->state = ACQUIRING;
      lock->retry_receiver = pthread_self();
      pthread_mutex_unlock(&mutex);
      tprintf("[%d] Acquire remote lock[%llu]", rlock_port, lid);
      ret = Acquire_remote(lid);
      pthread_mutex_lock(&mutex);
      if (ret == lock_protocol::OK) {
        tprintf("[%d], acquire lock ok\n", rlock_port)
        lock->state = LOCKED;
      } else {
        // wait retry signal
        tprintf("[%d], acquire lock waiting\n", rlock_port)
        while (lock->state != LOCKED) {
          pthread_cond_wait(&te->cv, &mutex);
        }
      }
    } else if (lock->state == FREE) {
      lock->state = LOCKED;  // Acquired the lock
      tprintf("[%d], acquire lock free\n", rlock_port)
    } else {
      tprintf("[%d], acquire lock[%llu], exception: %d\n", rlock_port, lid, lock->state);
    }
  } else {
    if (lock->state == NONE) {
      lock->state = ACQUIRING;
      lock->retry_receiver = pthread_self();
      pthread_mutex_unlock(&mutex);
      ret = Acquire_remote(lid);
      pthread_mutex_lock(&mutex);
      if (ret == lock_protocol::OK) {
        lock->state = LOCKED;
        tprintf("[%d], acquire lock ok 0\n", rlock_port)
      } else {
        // wait retry signal
        tprintf("[%d], acquire lock waiting 0\n", rlock_port)  
        while (lock->state != LOCKED) {
          pthread_cond_wait(&te->cv, &mutex);
        }
        tprintf("[%d], acquire lock wake from wait 0\n", rlock_port)
      }
    } else if (lock->state == FREE) {
      lock->state = LOCKED;
      tprintf("[%d], acquire lock free 0\n", rlock_port)
    } else {
      tprintf("Header acquire lock[%llu], state exception %d\n", lid, lock->state);
    }
  }
  pthread_mutex_unlock(&mutex);

  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{ 
  int ret = lock_protocol::OK;
  pthread_mutex_lock(&mutex);

  lock_client_cache::lock_entry *lock = locks[lid];
  if (lock == NULL) {
    tprintf("Release lock[%llu], lock not found\n", lid);
  }
  tprintf("Release lock[%llu] client[%d]\n", lid, rlock_port);
  if(lock->revoked) {
    // check whether there are some threads waiting for lock
    // grant at most MAX_TIME_TO_REVOKE threads
    if (lock->TIME_TO_REVOKE < MAX_TIME_TO_REVOKE && lock->threads.size() > 1) {
      tprintf("[%d] Release lock[%llu], TTR: %d \n", rlock_port, lid, lock->TIME_TO_REVOKE);
      lock->state = FREE;
      lock->threads.pop_front();
      lock->TIME_TO_REVOKE++;
      pthread_cond_signal(&lock->threads.front()->cv);
    } else {
      tprintf("[%d] Release lock remote[%llu], TTR: %d, state[%d]\n", rlock_port, lid, lock->TIME_TO_REVOKE, lock->state);
      lock->state = RELEASING;
      pthread_mutex_unlock(&mutex);
      ret = Release_remote(lid);
      pthread_mutex_lock(&mutex);
      lock->state = NONE;
      lock->revoked = false;
      lock->TIME_TO_REVOKE = 0;
      lock->threads.pop_front();
      if (lock->threads.size() > 0) {
        pthread_cond_signal(&lock->threads.front()->cv);
        tprintf("wake up next thread size: %lu \n", lock->threads.size());
      }
    }
    pthread_mutex_unlock(&mutex);
  } else {
    tprintf("[%d] Release lock[%llu], locally\n", rlock_port, lid);
    lock->state = FREE;
    lock->threads.pop_front();
    if (lock->threads.size() > 0) {
      pthread_cond_signal(&lock->threads.front()->cv);
    }
    pthread_mutex_unlock(&mutex);
  }
  
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&mutex);
  lock_entry *lock = locks[lid];
  if (lock == NULL) {
    tprintf("Revoke lock[%llu], lock not found\n", lid);
  }
  if (lock->state == FREE ) {
    if (lock->threads.size() == 0) {
      lock->state = RELEASING;
      pthread_mutex_unlock(&mutex);
      Release_remote(lid);
      pthread_mutex_lock(&mutex);
      lock->state = NONE;
      if (lock->threads.size() > 0)
      {
        pthread_cond_signal(&lock->threads.front()->cv);
      }
    tprintf("[%d] Revoke Free, release lock[%llu] size: %lu\n", rlock_port, lid, lock->threads.size());
    } else {
      lock->revoked = true;
    }
    
  } else if (lock->state == LOCKED || lock->state == ACQUIRING) {
    // lock->state == ACQUIRING 
    // happened when revoke_handler happens before acquire response from server
    // mark the lock as revked, after MAX_TIME_TO_REVOKE, release the lock
    lock->revoked = true;
    tprintf("[%d] Revoke[%llu], state is %d\n", rlock_port, lid, lock->state);
  } else {
    // lock->state == NONE || lock->state == RELEASING
    // happened when revoke response lost in the way to server
    tprintf("[%d] Revoke[%llu], lock state is none or releasing\n", rlock_port, lid);
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, int TTR,
                                 int &)
{
  int ret = rlock_protocol::OK;
  tprintf("client[%d] receiverd RETRY\n", rlock_port);
  pthread_mutex_lock(&mutex);
  lock_entry *lock = locks[lid];
  if (lock == NULL) {
    tprintf("Retry lock[%llu], lock not found\n", lid);
  }
  if (lock->state != ACQUIRING) {
    tprintf("[%d] Retry lock[%llu], lock state is not ACQUIRING\n",rlock_port, lid);
  }
  if (lock->threads.size() <= 0) {
    tprintf("[%d] Retry lock[%llu], lock header not found\n",rlock_port, lid);
  }

  if (lock->retry_receiver == lock->threads.front()->id) {
    tprintf("[%d] Retry lock[%llu], signa, size: %lu, TTR: %d\n", rlock_port, lid, lock->threads.size(), TTR);
    if (TTR > 0) {
      lock->revoked = true;
    }
    lock->state = LOCKED;
    pthread_cond_signal(&lock->threads.front()->cv);
  } else {
    tprintf("[%d] Retry lock[%llu], retry_receiver & threads front not match\n",rlock_port, lid);
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status 
lock_client_cache::Acquire_remote(lock_protocol::lockid_t lid) {
  // acquire until response is RETRY / OK
  int r;
  lock_protocol::status ret = lock_protocol::OK;
  do
  {
    ret = cl->call(lock_protocol::acquire, lid, id, r);
  } while (ret != lock_protocol::OK && ret != lock_protocol::RETRY);
  return ret;
}

lock_protocol::status
lock_client_cache::Release_remote(lock_protocol::lockid_t lid) {
  // release until response is OK
  int r;
  lock_protocol::status ret = lock_protocol::OK;
  do
  {
    ret = cl->call(lock_protocol::release, lid, id, r);
  } while (ret != lock_protocol::OK);
  return ret;
}
