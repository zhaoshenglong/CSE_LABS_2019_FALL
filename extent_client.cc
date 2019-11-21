// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>


extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);

  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

// a demo to show how to use RPC
extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::create, type, id);
  if (ret == extent_protocol::OK) {
    extent_protocol::filewithattr *file = new extent_protocol::filewithattr();
    file->filecontent = "";
    file->fileattr.size = 0;
    file->fileattr.type = type;
    file->fileattr.atime = time(NULL);
    file->fileattr.ctime = time(NULL);
    file->fileattr.mtime = time(NULL);
    clcache[id] = file;
    printf("extent client: create remote, inode: %llu\n", id);
  } else {
    printf("extent client: create failed\n");
  }
  printf("extent client create inode[%llu], ret: %d\n", id, ret);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  extent_protocol::filewithattr *file = clcache[eid];
  if (file == NULL) {
    file = new extent_protocol::filewithattr();
    extent_protocol::filewithattr filetmp;
    ret = cl->call(extent_protocol::getwithattr, eid, filetmp);
    if (ret == extent_protocol::OK) {
      file->fileattr.atime = filetmp.fileattr.atime;
      file->fileattr.type = filetmp.fileattr.type;
      file->fileattr.ctime = filetmp.fileattr.ctime;
      file->fileattr.mtime = filetmp.fileattr.mtime;
      file->fileattr.size = filetmp.fileattr.size;
      file->filecontent = filetmp.filecontent;
      clcache[eid] = file;
      printf("extent client: get remote %llu, content: %s\n", eid, filetmp.filecontent.c_str());
    } else {
      printf("get attr failed\n");
    }
  } else {
    file->fileattr.atime = time(NULL);
    printf("extent client: get locally: buf: %s\n", buf.c_str());
  }
  buf.assign(file->filecontent.data(), file->fileattr.size);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  extent_protocol::filewithattr *file = clcache[eid];
  if (file == NULL) {
    file = new extent_protocol::filewithattr();
    extent_protocol::filewithattr filetmp;
    ret = cl->call(extent_protocol::getwithattr, eid, filetmp);
    if (ret == extent_protocol::OK) {
      file->fileattr.atime = filetmp.fileattr.atime;
      file->fileattr.type = filetmp.fileattr.type;
      file->fileattr.ctime = filetmp.fileattr.ctime;
      file->fileattr.mtime = filetmp.fileattr.mtime;
      file->fileattr.size = filetmp.fileattr.size;
      file->filecontent = filetmp.filecontent;
      clcache[eid] = file;
    } else {
      printf("extent client getwithattr failed, inode(%llu)\n", eid);
    }
  } 
  attr.atime = file->fileattr.atime;
  attr.ctime = file->fileattr.ctime;
  attr.mtime = file->fileattr.mtime;
  attr.size = file->fileattr.size;
  attr.type = file->fileattr.type;

  // update mtime
  attr.mtime = time(NULL);
  file->fileattr.mtime = attr.mtime;
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf) 
{
  int r;
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here

  extent_protocol::filewithattr *file = clcache[eid];
  if (file == NULL) {
    if (eid != 1) {
    } else {
      printf("extent client: put root inode(%llu) remote \n", eid);
      extent_protocol::filewithattr file;
      ret = cl->call(extent_protocol::getwithattr, eid, file);
      clcache[eid] = new extent_protocol::filewithattr();
      clcache[eid]->filecontent = file.filecontent;
      clcache[eid]->fileattr.atime = file.fileattr.atime;
      clcache[eid]->fileattr.ctime = file.fileattr.ctime;
      clcache[eid]->fileattr.mtime = file.fileattr.mtime;
      clcache[eid]->fileattr.size = file.fileattr.size;
      clcache[eid]->fileattr.type = file.fileattr.type;
    }
  } else {
    file->filecontent = buf;
    file->fileattr.size = buf.size();
    file->fileattr.ctime = time(NULL);
    
    printf("extent client: put inode(%llu) locally, content: %s\n", eid, buf.c_str());
  }
  
  return ret;
}
extent_protocol::status 
extent_client::remove(extent_protocol::extentid_t eid)
{
  int r;
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::remove, eid, r);
  extent_protocol::filewithattr *file = clcache[eid];
  if (file == NULL) {
    printf("extent_client: remove: inode not exists locally\n");
  } else {
    printf("extent_client: remove inode(%llu) locally\n", eid);
  
    clcache.erase(clcache.find(eid));
  }
  return ret;
}

extent_protocol::status 
extent_client::flush(extent_protocol::extentid_t eid) {
  int r;
  extent_protocol::status ret = extent_protocol::OK;
  extent_protocol::filewithattr *file = clcache[eid];
  if (file == NULL) {
    printf("extent client: inode not found, flush abort\n");
  } else {
    ret = cl->call(extent_protocol::put, eid, file->filecontent, r);
    printf("flush: data: %s\n", file->filecontent.data());
    if (ret == extent_protocol::OK) {
      clcache.erase(clcache.find(eid));
      printf("extent client: flush inode: %llu OK  \n", eid);
    } else {
      printf("extent client: flush inode: %llu failed  \n", eid);
    }    
  }
  return ret;
}
