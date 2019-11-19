// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"


// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
 private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;

  enum lock_state {
    NONE,
    FREE,
    LOCKED,
    ACQUIRING,
    RELEASING
  };

  class thread_entry {
    public:
      pthread_t id;
      pthread_cond_t cv;
  };

  class lock_entry {
    public:
      lock_state state;
      std::list<thread_entry* >  threads;
      bool revoked;
      pthread_t retry_receiver;
      int TIME_TO_REVOKE;

      lock_entry(){
        state = NONE;
        threads = std::list<thread_entry* >();
        revoked = false;
        TIME_TO_REVOKE = 0;
      };
  };

  pthread_mutex_t mutex;

  std::map<lock_protocol::lockid_t, lock_client_cache::lock_entry*> locks;

 public:
  static int last_port;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, int,
                                       int &);
  lock_protocol::status Acquire_remote(lock_protocol::lockid_t);
  lock_protocol::status Release_remote(lock_protocol::lockid_t);
};


#endif
