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
  printf("extent client create inode[%llu], ret: %d\n", id, ret);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::get, eid, buf);
  printf("extent client get inode[%llu], ret: %d\n", eid, ret);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::getattr, eid, attr);
  printf("extent client getattr inode[%llu], ret: %d\n", eid, ret);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  int r;
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::put, eid, buf, r);
  printf("extent client put inode[%llu], ret: %d\n", eid, ret);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  int r;
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::remove, eid, r);
  printf("extent client remove inode[%llu], ret: %d\n", eid, ret);
  return ret;
}


