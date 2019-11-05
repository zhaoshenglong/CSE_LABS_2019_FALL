// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

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

  mutex = PTHREAD_MUTEX_INITIALIZER;
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  int r;

  pthread_mutex_lock(&mutex);
  lock_client_cache::lock_entry *lock;
  if (locks.find(lid) == locks.end()) 
    locks[lid] = new lock_entry();
  lock = locks[lid];

  bool acquiring = false;
  if(lock->state == NONE) {
    lock->state = ACQUIRING;
    acquiring = true;
  } else if (lock->state == FREE) {
    lock->state = LOCKED;
  } else {
    do {
      pthread_cond_wait(&lock->cv, &mutex);
    } while (lock->state != NONE && lock->state != FREE);
    if(lock->state == NONE) {
      acquiring = true;
      lock->state = ACQUIRING;
    } else {
      lock->state = LOCKED;
    }
  }
  pthread_mutex_unlock(&mutex);

  if(acquiring) {
    lock_protocol::status ret = cl->call(lock_protocol::acquire, lid, id, r);
  
    while (ret != lock_protocol::OK){
      pthread_mutex_lock(&mutex);
      if (lock->state == LOCKED){
        pthread_mutex_unlock(&mutex);
        break;
      }
      pthread_mutex_unlock(&mutex);
      if (ret != lock_protocol::RETRY){
        ret = cl->call(lock_protocol::acquire, lid, id, r);
      }
    }
    pthread_mutex_lock(&mutex);
    lock->state = LOCKED;
    pthread_mutex_unlock(&mutex);
  }
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{ 
  int ret = rlock_protocol::OK;
  int r;

  pthread_mutex_lock(&mutex);
  bool releasing = false;
  lock_entry *lock = locks[lid];

  if (lock == NULL || lock->state != LOCKED) {
    tprintf("release a unacquired lock\n");
    return lock_protocol::NOENT;
  } 
  
  if(lock->REVOKE) {
    // RELEASE 
    lock->state = RELEASING;
    lock->REVOKE = false;
    releasing = true;
  } else {
    // free lock and wake up others
    tprintf("client[%d] release lock[%llu] FREE\n", rlock_port, lid);
    lock->state = FREE;
    pthread_cond_signal(&lock->cv);
  }
  
  pthread_mutex_unlock(&mutex);

  if (releasing) {
    do {
      ret = cl->call(lock_protocol::release, lid, id, r);
    } while (ret != lock_protocol::OK);
        
    tprintf("client[%d] release lock[%llu] NONE\n", rlock_port, lid);
    pthread_mutex_lock(&mutex);
    lock->state = NONE;
    pthread_mutex_unlock(&mutex);
    pthread_cond_signal(&lock->cv);
  }
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  int r;
  pthread_mutex_lock(&mutex);
  bool releasing = false;
  lock_entry *lock = locks[lid];
  if(lock == NULL || lock->state == NONE) {
    tprintf("revoke lock not found\n");
    return lock_protocol::RPCERR;
  }

  if (lock->state == FREE) {
    lock->state = RELEASING;
    lock->REVOKE = false;
    releasing = true;
  } else if(lock->state == NONE) {
    
  } else {
    // this state must be locked
    lock->REVOKE = true;
  }
  pthread_mutex_unlock(&mutex);

  if (releasing) {
    do {
      ret = cl->call(lock_protocol::release, lid, id, r);
    } while (ret != lock_protocol::OK);
        
    tprintf("client[%d] release lock[%llu] NONE\n", rlock_port, lid);
    pthread_mutex_lock(&mutex);
    lock->state = NONE;
    pthread_mutex_unlock(&mutex);
    pthread_cond_signal(&lock->cv);
  }
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&mutex);
  lock_entry *lock = locks[lid];
  if(lock == NULL) {
    tprintf("retry lock not found");
    return ret;
  }

  if(lock->state == ACQUIRING) {
    lock->state = LOCKED;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}



