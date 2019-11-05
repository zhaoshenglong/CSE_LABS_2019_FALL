// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
  nacquire = 0;
  mutex = PTHREAD_MUTEX_INITIALIZER;
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  lock_entry *lock;
  
  pthread_mutex_lock(&mutex);
  if(locks.find(lid) == locks.end())
    locks[lid] = new lock_entry();
  lock = locks[lid];
  
  handle *h;

  switch (lock->state)
  {
  case FREE:
    lock->state = GRANTED;
    lock->owner = id;
    pthread_mutex_unlock(&mutex);
    return lock_protocol::OK;
  case GRANTED:
    lock->waiting_clients.push_back(id);
    lock->state = REVOKING;
    h = new handle(lock->owner);
    pthread_mutex_unlock(&mutex);
    ret = h->safebind()->call(rlock_protocol::revoke, lid, r);
    return lock_protocol::RETRY;
  case REVOKING:
    lock->waiting_clients.push_back(id);
    pthread_mutex_unlock(&mutex);
    return lock_protocol::RETRY;
  default:
    assert(0);
    break;
  }
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{ 
  pthread_mutex_lock(&mutex);
  lock_entry *lock = locks[lid];
  
  if(!lock || lock->owner.compare(id)) {
    return lock_protocol::RPCERR;
  }

  if (lock->waiting_clients.empty())
  {
    lock->state = FREE;
    lock->owner = "";
    pthread_mutex_unlock(&mutex);
    return lock_protocol::OK;
  }
  
  lock->state = GRANTED;
  lock->owner = lock->waiting_clients.front();
  lock->waiting_clients.pop_front();
  handle h(lock->owner);
  
  pthread_mutex_unlock(&mutex);
  
  rlock_protocol::status rret = rlock_protocol::OK;
  do{
    rret = h.safebind()->call(rlock_protocol::retry, lid, r);
  } while(rret != rlock_protocol::OK);

  pthread_mutex_lock(&mutex);
  if (!lock->waiting_clients.empty()) {
    handle h(lock->owner);
    pthread_mutex_unlock(&mutex);
    h.safebind()->call(rlock_protocol::revoke, lid, r);
  } else {
    pthread_mutex_unlock(&mutex);
  }
  
  return lock_protocol::OK;
}


lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

