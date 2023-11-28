//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// inode.h
//
// Identification: src/include/filesystem/operation.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "metadata/manager.h"
#include <sys/stat.h>

namespace chfs {

/**
 * Implement the basic inode filesystem
 */
class FileOperation {
  friend class MetadataServer;
protected:
  // Feel free to remove them if you don't want to implement the inode-based
  // filesystem
  [[maybe_unused]] std::shared_ptr<BlockManager> block_manager_;
  [[maybe_unused]] std::shared_ptr<InodeManager> inode_manager_;
  [[maybe_unused]] std::shared_ptr<BlockAllocator> block_allocator_;

public:
  /**
   * Initialize a filesystem from scratch
   * @param bm the block manager to manage the block device
   * @param max_inode_supported the maximum number of inodes supported by the
   * filesystem
   */
  FileOperation(std::shared_ptr<BlockManager> bm, u64 max_inode_supported);

  /**
   * Create a filesystem handler from an initialized filesystem
   *
   * Note that this function will consider the filesystem as not corrupted
   */
  static auto create_from_raw(std::shared_ptr<BlockManager> bm)
      -> ChfsResult<std::shared_ptr<FileOperation>>;

  /**
   * Get the free inodes of the filesystem.
   * Will read the inode bitmap of the underlying filesystem
   */
  auto get_free_inode_num() const -> ChfsResult<u64>;

  // Data path operations

  /**
   * Create a inode with a given type
   * It will allocate a block for the created inode
   *
   * @param type the type of the inode
   * @return the id of the inode
   */
  auto alloc_inode(InodeType type) -> ChfsResult<inode_id_t>;

  /**
   * Get the file attribute of the given inode
   *
   * @param id the id of the inode
   * @return INVALID_ARG if the inode id is invalid
   */
  auto getattr(inode_id_t id) -> ChfsResult<FileAttr>;

  /**
   * Get the type of the given inode
   * @param id the id of the inode
   * @return INVALID_ARG if the inode id is invalid
   */
  auto gettype(inode_id_t id) -> ChfsResult<InodeType>;

  /**
   * The combined version of the above APIs
   */
  auto get_type_attr(inode_id_t id)
      -> ChfsResult<std::pair<InodeType, FileAttr>>;

  /**
   * Write the content to the blocks pointed by the inode
   * If the inode's block is insufficient, we will dynamically allocate more
   * blocks
   *
   * @param id the id of the inode
   * @param content the content to write
   */
  auto write_file(inode_id_t, const std::vector<u8> &content) -> ChfsNullResult;

  /**
   * Write the content to the blocks pointed by the inode
   * If the inode's block is insufficient, we will dynamically allocate more
   * blocks.
   *
   * if off > file size, we will fill the gap with 0
   *
   * @return the number of bytes written
   */
  auto write_file_w_off(inode_id_t id, const char *data, u64 sz, u64 offset)
      -> ChfsResult<u64>;

  /**
   * Read the content to the blocks pointed by the inode
   *
   * @param id the id of the inode
   */
  auto read_file(inode_id_t) -> ChfsResult<std::vector<u8>>;

  /**
   * Read the content to the blocks pointed by the inode
   *
   * # Note
   * We assume that the offset + size <= file size
   */
  auto read_file_w_off(inode_id_t id, u64 sz, u64 offset)
      -> ChfsResult<std::vector<u8>>;

  /**
   * Remove the file corresponding to an inode.
   * It is defined in contorl_op.cc
   *
   * @param id the id of the inode
   * @return whether the remove is ok
   */
  auto remove_file(inode_id_t) -> ChfsNullResult;

  /**
   * Get the free blocks of the filesystem.
   * Will read the block bitmap of the underlying filesystem
   */
  auto get_free_blocks_num() const -> ChfsResult<u64>;

  /**
   * Lookup the directory
   */
  auto lookup(inode_id_t, const char *name) -> ChfsResult<inode_id_t>;

  /**
   * Helper function to create directory or file
   *
   * @param parent the id of the parent
   * @param name the name of the directory
   */
  auto mk_helper(inode_id_t parent, const char *name, InodeType type)
      -> ChfsResult<inode_id_t>;

  /**
   * Create a directory at the parent
   *
   * @param parent the id of the parent
   * @param name the name of the directory
   */
  auto mkdir(inode_id_t parent, const char *name) -> ChfsResult<inode_id_t> {
    return mk_helper(parent, name, InodeType::Directory);
  }

  /**
   * Create a file at the parent
   *
   * @param parent the id of the parent
   * @param name the name of the directory
   */
  auto mkfile(inode_id_t parent, const char *name) -> ChfsResult<inode_id_t> {
    return mk_helper(parent, name, InodeType::FILE);
  }

  /**
   * Resize the content of the inode
   * Assumption: it operates on the file, but not the directory
   *
   */
  auto resize(inode_id_t id, u64 sz) -> ChfsResult<FileAttr>;

  /**
   * Remove the file named @name from directory @parent.
   * Free the file's blocks.
   *
   * @return  If the file doesn't exist, indicate error ENOENT.
   * @return  ENOTEMPTY if the deleted file is a directory
   */
  auto unlink(inode_id_t parent, const char *name) -> ChfsNullResult;

  // [transaction] //
  /**
   * mknode function to create directory or file atomically
   * 
   * @param parent the id of the parent
   * @param name the name of the directory
   * @param type node type
   * @param tx_ops vector to store all block operation
   */
  auto mknode_atomic(inode_id_t parent, const char *name, InodeType type, std::vector<std::shared_ptr<BlockOperation>> &tx_ops)
      -> ChfsResult<inode_id_t>;

  /**
   * Create a inode with a given type atomically
   * It will allocate a block for the created inode
   *
   * @param type the type of the inode
   * @param tx_ops vector to store all block operation
   * @return the id of the inode
   */
  auto alloc_inode_atomic(InodeType type, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) 
      -> ChfsResult<inode_id_t>;

  /**
   * Write the content to the blocks pointed by the inode atomically
   * If the inode's block is insufficient, we will dynamically allocate more
   * blocks
   *
   * @param id the id of the inode
   * @param content the content to write
   * @param tx_ops vector to store all block operation
   */
  auto write_file_atomic(inode_id_t, const std::vector<u8> &content, std::vector<std::shared_ptr<BlockOperation>> &tx_ops)
      -> ChfsNullResult;

  /**
   * Remove the file named @name from directory @parent.
   * Free the file's blocks.
   *
   * @return  If the file doesn't exist, indicate error ENOENT.
   * @return  ENOTEMPTY if the deleted file is a directory
   */
  auto unlink_atomic(inode_id_t parent, const char *name, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsNullResult;

  /**
   * Remove the file corresponding to an inode.
   * It is defined in contorl_op.cc
   *
   * @param id the id of the inode
   * @return whether the remove is ok
   */
  auto remove_file_atomic(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsNullResult;

  auto lookup_from_memory(inode_id_t, const char *name, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsResult<inode_id_t>;

  auto read_file_from_memory(inode_id_t, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsResult<std::vector<u8>>;

  auto gettype_from_memory(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsResult<InodeType>;
  // [transaction] //

private:
  FileOperation(std::shared_ptr<BlockManager> bm,
                std::shared_ptr<InodeManager> im,
                std::shared_ptr<BlockAllocator> ba)
      : block_manager_(bm), inode_manager_(im), block_allocator_(ba) {}
};

} // namespace chfs