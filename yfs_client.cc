// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    } 

    return false;
}


int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    printf("setattr: %lu\n", size);
    extent_protocol::attr a;
    if (ec->getattr(ino, a) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }
    if (a.type == 0) {
        r = NOENT;
        return r;
    }

    std::string buf;
    ec->get(ino, buf);

    std::string data;
    if (size < a.size) {
        data = buf.substr(0, size);
    } else if (size > a.size) {
        data = buf + std::string(size - a.size, '\0');
    } else {
        // ... do nothing
    }

    printf("setattr: ino: %llu, size: %lu, asize: %lu, content: %s\n",ino, size, a.size,  data.c_str());
    ec->put(ino, data);
    
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found = false;
    if (lookup(parent, name, found, ino_out) != extent_protocol::OK) {
        if (found) r = EXIST;
        else r = IOERR;
        return r;
    }
    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }
    // get parent attr
    extent_protocol::attr a;
    ec->getattr(parent, a);

    // get parent content;
    std::string buf;
    ec->get(parent, buf);
   
    // append created entry to parent;
    buf.append(name);
    buf.append("/");
    buf.append(filename(ino_out));
    buf.append("/");

    // store parent
    ec->put(parent, buf);

    printf("create file, inum: %llu, name: %s\n", ino_out, name);
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    bool found = false;
    if (lookup(parent, name, found, ino_out) != extent_protocol::OK) {
        if (found) r = EXIST;
        else r = IOERR;
        return r;
    }
    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }

    // get parent attr
    extent_protocol::attr a;
    ec->getattr(parent, a);

    // get parent content;
    std::string buf;
    ec->get(parent, buf);
    
    // append created entry to parent;
    buf.append(name);
    buf.append("/");
    buf.append(filename(ino_out));
    buf.append("/");

    // store parent
    ec->put(parent, buf);    

    printf("make dir, inum: %llu, name: %s\n", ino_out, name);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    // directory content format is array of struct dirent
    // use std::string is a little bit waste of space

    // get parent file entry
    std::list<dirent> entries;
    readdir(parent, entries);
    for (std::list<struct dirent>::iterator it = entries.begin(); it != entries.end(); it++) {
        printf("look up, file name: %s, file inum: %llu\n", it->name.c_str(), it->inum);
        if ( !it->name.compare(name) ) {
            found = true;
            ino_out = it->inum;
            r = EXIST;
            return r;
        }
    }

    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    // get parent attr
    extent_protocol::attr a;
    ec->getattr(dir, a);

    // get parent content;
    std::string buf;
    ec->get(dir, buf);
    
    unsigned int i = 0;

    std::string name, inum;
    
    printf("buf: \n%s\n", buf.c_str());
    int head = 0, tail = 0;
    while ( i < buf.size() ){
        head = i; 
        tail = buf.find('/', head);

        dirent ent;
        name=buf.substr(head, tail - head);

        head = tail + 1;
        tail = buf.find("/", head);
        inum=buf.substr(head,  tail - head);

        ent.name=name;
        ent.inum=n2i(inum);

        printf("read dir, dir: %llu, inum: %llu, name: %s,\n", dir, ent.inum, ent.name.c_str());
        list.push_back(ent);

        i = tail + 1;
    }
    

    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    extent_protocol::attr a;
    if (ec->getattr(ino, a) != OK) {
        r = IOERR;
        return r;
    }
    if (a.type == 0) {
        r = NOENT;
        return r;
    }

    std::string buf;
    ec->get(ino, buf);

    if (off < a.size) {
        data = buf.substr(off, size < a.size - off ? size : a.size - off);
    } else {
        r = IOERR;
        return r;
    }

    printf("read file, ino: %llu, size: %lu, off: %lu, data: %s\n", ino, size, off, data.c_str());

    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    extent_protocol::attr a;
    if (ec->getattr(ino, a) != OK) {
        r = IOERR;
        return r;
    }
    if (a.type == 0) {
        r = NOENT;
        return r;
    }

    printf("write file, originale size: %u\n", a.size);
    int holes = 0;
    if (off > a.size) {
        holes = off - a.size;
    
    }
    std::string fill_hole(holes, '\0');

    std::string file_data;
    ec->get(ino, file_data);

    std::string write_bytes ;
    for (uint i = 0; i < size; i++ ) {
        write_bytes += data[i];
    }
    if (holes) {
        file_data.append(fill_hole);
        file_data.append(write_bytes);
        printf("FILE DATA:, a.size: %u,file_data.size: %lu,  off: %lu, data: %s, fill_holes: %d\n", a.size, file_data.size(), off, write_bytes.c_str(), holes);
    } else {
        std::string right_str;
        if (off + size < a.size) {
            right_str = file_data.substr(off + size, a.size);
        }
        file_data = file_data.substr(0, off) + write_bytes 
            + right_str;
        printf("FILE DATA:, a.size: %d,file_data.size: %lu,  off: %lu, data: %s\n", a.size, file_data.size(), off, write_bytes.c_str());
        
    }
    bytes_written = size + holes;
    printf("write file, ino: %llu, size: %lu, off: %lu, data: %s\n", ino, size, off, write_bytes.c_str());

    ec->put(ino, file_data);

    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    
    std::string buf;
    if (ec->get(parent, buf) != OK) {
        r = NOENT;
        return r;
    }

    inum ino;
    
    unsigned int i = 0; bool found = false;

    std::string name_s, inum_s;

    int head = 0, tail = 0, begin, end;

    printf("unlink, buf: %s,\n", buf.c_str());
    while ( i < buf.size() && !found ){
        head = i; 
        tail = buf.find('/', head);

    
        name_s=buf.substr(head, tail - head);

        head = tail + 1;
        tail = buf.find("/", head);

        inum_s=buf.substr(head,  tail - head);

        ino=n2i(inum_s);

        if (!name_s.compare(name)) {
            found = true;
            begin = i;
            end = tail + 1;
        }
        printf("unlink, found ino: %llu, name: %s, name_s: %s\n", ino, name, name_s.c_str());

        i = tail + 1;
    }

    if (found) {
        if (ec->remove(ino) != OK ) {
            r = IOERR;
            return r;
        }
        // update parent
        buf = buf.substr(0, begin) + buf.substr(end, buf.size());
        printf("unlink, parent:%llu, buf:%s,\n", parent, buf.c_str());
        ec->put(parent, buf);
    } else {
        printf("file not exists, cannot remove\n");
        r = NOENT;
        return r;
    }

    return r;
}

int 
yfs_client::symlink(inum parent, const char *name, const char *link, inum& ino_out) {
    int r = OK;
    /**
     * check if file already exists, 
     * check if link exists ? 
     * create symbolic link
     * update parent
     */ 
    bool found;
    if ( (r = lookup(parent, name, found, ino_out) != OK) ) {
        if (r == EEXIST && found == true) 
            r = EEXIST; 
        else 
            r = IOERR;
        return r;
    }

    if (ec->create(extent_protocol::T_SYMLINK, ino_out) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }

    if(ec->put(ino_out, std::string(link)) != OK) {
        r = IOERR;
        printf("create symbolic link, error! inum: %llu, name: %s, link: %s\n", ino_out, name, link);
        return r;
    }

    // get parent attr
    extent_protocol::attr a;
    ec->getattr(parent, a);

    // get parent content;
    std::string buf;
    ec->get(parent, buf);
   
    // append created entry to parent;
    buf.append(name);
    buf.append("/");
    buf.append(filename(ino_out));
    buf.append("/");

    // store parent
    ec->put(parent, buf);

    printf("create symbolic link, inum: %llu, name: %s, link: %s\n", ino_out, name, link);
    return r;
}
int 
yfs_client::readlink(inum ino, std::string &link) {
        printf("read symbolic link: %s \n", "fuck");

    if (ec->get(ino, link) != OK) {
        printf("read symbolic link: %s \n", link.data());
        return IOERR;
    }
    printf("read symbolic link: %s\n", link.data());
    return OK;
}