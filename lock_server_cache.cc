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
  pthread_mutex_init(&mutex, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &r)
{
  lock_entry *lock; 
  pthread_mutex_lock(&mutex);
  if(locks.find(lid) == locks.end())
    locks[lid] = new lock_entry();
  lock = locks[lid];
  tprintf("Acquire lock[%llu] from client[%s], state is %d\n", lid, id.c_str(), lock->state);
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
    pthread_mutex_unlock(&mutex);
    Revoke_remote(lock->owner, lid);
    return lock_protocol::RETRY;
  case REVOKING: case GRANTING:
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
  
  if(lock == NULL || lock->owner.compare(id)) {
    return lock_protocol::RPCERR;
  }
  tprintf("Release lock[%llu] from client[%s], state is %d, waiting: %lu\n", lid, id.c_str(), lock->state, lock->waiting_clients.size());
  if (lock->waiting_clients.empty())
  {
    lock->state = FREE;
    lock->owner = "";
    pthread_mutex_unlock(&mutex);
    return lock_protocol::OK;
  }
  
  lock->state = GRANTING;
  lock->owner = lock->waiting_clients.front();
  lock->waiting_clients.pop_front();
  if (!lock->waiting_clients.empty()) {
    pthread_mutex_unlock(&mutex);
    Retry_remote(lock->owner, lid, 1);
    tprintf("\tRelease, retry client[%s], non empty\n", lock->owner.c_str());
  } else {
    lock->state = GRANTED;
    pthread_mutex_unlock(&mutex);
    Retry_remote(lock->owner, lid, -1);
    tprintf("\tRelease, retry client[%s], empty\n", lock->owner.c_str());
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

rlock_protocol::status 
lock_server_cache::Retry_remote(std::string owner, lock_protocol::lockid_t lid, int ttr) 
{
  rlock_protocol::status ret = rlock_protocol::OK;
  int r;
  handle h(owner);
  rpcc *cl = h.safebind();
  do
  {
    ret = cl->call(rlock_protocol::retry, lid, ttr, r);
    printf("retry from server %lu\n", ret);
  } while (ret != rlock_protocol::OK);
  return ret;
}

rlock_protocol::status 
lock_server_cache::Revoke_remote(std::string owner, lock_protocol::lockid_t lid) 
{
  rlock_protocol::status ret = rlock_protocol::OK;
  int r;
  handle h(owner);
  rpcc *cl = h.safebind();
  do
  {
    ret = cl->call(rlock_protocol::revoke, lid, r);
    printf("revoke from server ret: %lu\n", ret);
  } while (ret != rlock_protocol::OK);
  return ret;
}
