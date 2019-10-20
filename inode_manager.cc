#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  // start from IBLOCK(0, BLOCK_NUM);
  
  std::map<blockid_t, int>::iterator it;
  blockid_t id = 0;
  for (id = BLOCK_NUM / BPB + INODE_NUM + 3; id < BLOCK_NUM; id++) {
    it = using_blocks.find(id);
    if (it == using_blocks.end()) {
      
      char buf[BLOCK_SIZE];
      int bblock = BBLOCK(id);

      d->read_block(bblock, buf);

      char byte = buf[(id % BPB) / 8];
      buf[(id % BPB) / 8] = byte | (0x1 << ((id % BPB) % 8 ));
      d->write_block(bblock, buf);
      using_blocks.insert(std::pair<blockid_t,int>(id, 1));

      break;
    }
  }
  return id;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */

  char buf[BLOCK_SIZE];
  int bblock = BBLOCK(id);

  d->read_block(bblock, buf);
  
  // clear bitmap
  char byte = buf[(id % BPB) / 8];
  buf[(id % BPB) / 8] = byte & (~(0x1 << ((id % BPB) % 8 )));
  d->write_block(bblock, buf);
  
  // clear block, set zero
  bzero(buf, BLOCK_SIZE);
  d->write_block(id, buf);

  // erase using_blocks; 
  using_blocks.erase(using_blocks.find(id));
  
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  inode_t *file = (inode_t*)malloc(sizeof(inode_t));
  bzero(file, sizeof(inode_t));

  file->type = type;
  time_t now = time(NULL);
  file->ctime = now;
  file->atime = now;
  file->mtime = now;
  file->size = 0;
  int inum = 1;

  inode_t *inode; 
  for (int i = 1; i < INODE_NUM; i++) {
    inode = get_inode(i);
    if (inode == NULL) {
      inum = i;
      put_inode(i, file);
      break;
    } else {
      free(inode);
    }
  }

  free(file);
  

  return inum;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode_t *inode = get_inode(inum);
  if (inode != NULL ) {
    bzero(inode, sizeof(inode_t));
    put_inode(inum, inode);
  }
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;
  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))
#define MAX(a,b) ((a)>(b)) ? (a) : (b)
/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  inode_t *fileIndex = get_inode(inum);
  
  if (!fileIndex) {
    printf("invalid inode when read file\n");
    return;
  }
  *size = fileIndex->size;
  int block_num = ((*size) + BLOCK_SIZE - 1) / BLOCK_SIZE;
  bool is_indirect = block_num > NDIRECT + 1;  
  printf("\tread block info, size: %d, blocks: %d, is_indirect: %d\n", *size, block_num, is_indirect);
  *buf_out = (char *)malloc(block_num * BLOCK_SIZE);
  bzero(*buf_out, block_num * BLOCK_SIZE);
  
  if (is_indirect) {
    for (uint32_t i = 0; i < NDIRECT; i++) {
      bm->read_block(fileIndex->blocks[i], (*buf_out) + BLOCK_SIZE * i);
      printf("read indirect block: %d, data: %s\n", fileIndex->blocks[i], (*buf_out) + (i) * BLOCK_SIZE);
    }

    blockid_t indirectBlock[BLOCK_SIZE / sizeof(blockid_t)];  
    bm->read_block(fileIndex->blocks[NDIRECT], (char *)indirectBlock);
    
    for (int i = 0; i < block_num - NDIRECT; i++) {
      bm->read_block(indirectBlock[i], (*buf_out) + (NDIRECT + i) * BLOCK_SIZE);
      printf("read indirect block, indirect block: %d: %d, data: %s\n", fileIndex->blocks[NDIRECT], indirectBlock[i], (*buf_out) + (NDIRECT + i) * BLOCK_SIZE);
    }
  } else {
    for (int32_t i = 0; i < block_num; i++) {
      bm->read_block(fileIndex->blocks[i], (*buf_out) + BLOCK_SIZE * i);
      printf("read direct block: %d\n", fileIndex->blocks[i]);
    }
  }

  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  if ( (size + BLOCK_SIZE -1) / BLOCK_SIZE > (int)MAXFILE ) {
    printf("maximum file size reached when write file\n");
    return;
  }

  inode_t *fileIndex = get_inode(inum);
  if (!fileIndex) {
    printf("invalid inode when write file\n");
    return;
  }
  bool is_origin_indirect = false, is_new_indirect = false;
  int oblocks_num = (fileIndex->size + BLOCK_SIZE -1) / BLOCK_SIZE, 
  nblocks_num = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  is_origin_indirect = oblocks_num > NDIRECT + 1;
  is_new_indirect = nblocks_num > NDIRECT + 1;
  oblocks_num += is_origin_indirect ? 1 : 0;
  nblocks_num += is_new_indirect ? 1 : 0;
  
  blockid_t blocks[MAX(oblocks_num, nblocks_num)];
  printf("start write file, size: %d\n", size);
  for (int32_t i = 0; i < MIN(NDIRECT + 1, oblocks_num); i++) {
    printf("blocks i: %d\n", fileIndex->blocks[i]);
    blocks[i] = fileIndex->blocks[i];
  }

  if (is_origin_indirect) {
    blockid_t block[BLOCK_SIZE];
    bm->read_block(blocks[NDIRECT], (char *)block);
    
    for (int32_t i = NDIRECT + 1; i < oblocks_num; i++) {
      blocks[i] = block[i -  - 1];
    }
  }

  if (nblocks_num > oblocks_num) {
    /**
     * if original blocks is less than new blocks,
     * then allocate blocks, record id in the blocks,
     * if new blocks is larger than max direct file, then allocate
     *  one more indirect block
     */ 

    if(is_new_indirect) {
    
      for (int32_t i = 0; i < nblocks_num - oblocks_num; i++) {
          blocks[oblocks_num + i] = bm->alloc_block();
          printf("n > o, indirect, blocks: %d, index: %d\n", blocks[oblocks_num + i], oblocks_num + i);
      }

    }
    /**
     * else, original blocks and new blocks both are not
     * indirect blocks; they can be simply allocate
     */ 
    else {
      for (int32_t i = 0; i < nblocks_num - oblocks_num; i++) {
          blocks[oblocks_num + i] = bm->alloc_block();
          printf("n > o, not indirect, allocate blocks: %d\n", blocks[oblocks_num + i]);
        }
    }
  } else if(nblocks_num < oblocks_num) {
    if(is_new_indirect) {
      for (int32_t i = nblocks_num + 1; i < oblocks_num; i++) {
        bm->free_block(blocks[i]);
        printf("p > n, indirect, free block: %d\n", blocks[i]);
      }
    } else {
      for (int32_t i = nblocks_num; i < oblocks_num; i++) {
        bm->free_block(blocks[i]);
        printf("p > n, not indirect, free block: %d\n", blocks[i]);
      }
      printf("enter into this part\n");
    }
  } else {
    // ...
    printf("new blocks equals original blocks\n");
  }

  if (is_new_indirect) {
    printf("is_new_indirect: %d\n", is_new_indirect);
    // write direct blocks;
    for (uint32_t i = 0; i < NDIRECT; i++) {
      bm->write_block(blocks[i], buf + i * BLOCK_SIZE);
    }
    
    // write indirect block contianer
    blockid_t tmp[BLOCK_SIZE / sizeof(blockid_t)];
    bzero(tmp, BLOCK_SIZE);
    for (int i = NDIRECT + 1; i < nblocks_num; i++) {
      tmp[i - NDIRECT - 1] = blocks[i];
    }
    bm->write_block(blocks[NDIRECT], (char *)tmp);
    
    // wirte content into indirect blocks
    for (int32_t i = NDIRECT; i < nblocks_num - 1; i++)
    {
      bm->write_block(blocks[i + 1], buf + i * BLOCK_SIZE);
      printf("write blocks: block id: %d, data: %s\n", blocks[i + 1],  buf + i * BLOCK_SIZE);
    }
    
  } else {
    printf("start write block\n");
    printf("is_new_indirect: %d\n", is_new_indirect);
    for (int32_t i = 0; i < nblocks_num; i++){
      printf("wirte block %d\n", blocks[i]);
      bm->write_block(blocks[i], buf + i * BLOCK_SIZE);
    }
  }
  
  
  for (int32_t i = 0; i < MIN(nblocks_num, NDIRECT + 1); i++) {
    fileIndex->blocks[i] = blocks[i];
  }

  time_t now = time(NULL);
  fileIndex->mtime=now;
  fileIndex->atime=now;
  fileIndex->size=size;
  fileIndex->ctime=now;
  put_inode(inum, fileIndex);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *file = get_inode(inum);
  if (!file) {
    a.type = 0;
    return;
  }

  a.atime = file->atime;
  a.ctime = file->ctime;
  a.mtime = file->mtime;
  a.size = file->size;
  a.type = file->type;
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  inode_t *fileIndex = get_inode(inum);

  int blocks_num = (fileIndex->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  bool is_indirect = blocks_num > NDIRECT + 1;

  if (is_indirect) {
    for (uint32_t i = 0; i < NDIRECT; i++) {
      bm->free_block(fileIndex->blocks[i]);
    }
    blockid_t indirectBlock[BLOCK_SIZE / sizeof(blockid_t)];
    bm->read_block(fileIndex->blocks[NDIRECT], (char *)indirectBlock);

    for(int32_t i = 0; i < blocks_num - NDIRECT; i++) {
      bm->free_block(indirectBlock[i]);
    }
  }  else {
    for(int32_t i = 0; i < blocks_num; i++) {
      bm->free_block(fileIndex->blocks[i]);
    }
  } 
  free_inode(inum);
  
  return;
}
