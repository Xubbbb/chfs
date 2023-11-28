//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// manager.h
//
// Identification: src/include/metadata/manager.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "./inode.h"
#include "block/allocator.h"

namespace chfs {

// inode should be larger than 0
const inode_id_t KInvalidInodeID = 0;

class FileOperation;

/**
 * The manager manages the inode.
 * This includes managing an inode table.
 *
 * At a high-level, the inode manager assumes the following layout on blocks:
 * | block 0     | block 1  ...  | block x ...             | block y ... | block
 * N  ...         |
 * | Super block | Inode Table   | Inode allocation bitmap |
 * Block allocation bitmap ... |  Other data blocks   |
 */
class InodeManager {
  friend class FileOperation;
  // We will modify the block manager
  std::shared_ptr<BlockManager> bm;
  u64 max_inode_supported;
  u64 n_table_blocks;
  u64 n_bitmap_blocks;

public:
  /**
   * Construct an InodeManager from scratch.
   * Note that it will initialize the blocks in the block manager.
   */
  InodeManager(std::shared_ptr<BlockManager> bm, u64 max_inode_supported);

  static auto to_shared_ptr(InodeManager m) -> std::shared_ptr<InodeManager> {
    return std::make_shared<InodeManager>(m);
  }

  /**
   * Construct an InodeManager from a block manager.
   * Note that it won't modify any blocks on the block manager.
   *
   * The max_inode_supported can be found in the super block.
   */
  static auto create_from_block_manager(std::shared_ptr<BlockManager> bm,
                                        u64 max_inode_supported)
      -> ChfsResult<InodeManager>;

  /**
   * Get the maximum number of inode supported.
   * The number is determined when the file system is created.
   */
  auto get_max_inode_supported() const -> u64 { return max_inode_supported; }

  /**
   * Allocate and initialize an inode with proper type
   * @param type: file type
   * @param bid: inode block ID
   */
  auto allocate_inode(InodeType type, block_id_t bid) -> ChfsResult<inode_id_t>;

  //[ transaction ]//
    /**
   * Allocate and initialize an inode with proper type atomically
   * @param type: file type
   * @param bid: inode block ID
   * @param tx_ops: vector to store all block operation
   */
  auto allocate_inode_atomic(InodeType type, block_id_t bid, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsResult<inode_id_t>;

  auto free_inode_atomic(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsNullResult;

  /**
   * Set the block ID of the inode
   * @param idx: **physical** inode ID
   */
  auto set_table_atomic(inode_id_t idx, block_id_t bid, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsNullResult;

  auto get_from_memory(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsResult<block_id_t>;

  auto read_inode_from_memory(inode_id_t id, std::vector<u8> &buffer, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsResult<block_id_t>;

  auto get_type_from_memory(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsResult<InodeType>;
  //[ transaction ]//

  /**
   * Get the number of free inodes
   * @return the number of free inodes if Ok
   */
  auto free_inode_cnt() const -> ChfsResult<u64>;

  /**
   * Get the block ID of the inode
   * @param id: **logical** inode ID
   *
   * Note that we don't check whether the returned block id is valid
   */
  auto get(inode_id_t id) -> ChfsResult<block_id_t>;

  /**
   * Free the inode entry id
   */
  auto free_inode(inode_id_t id) -> ChfsNullResult;

  /**
   * Get the attribute of the inode
   */
  auto get_attr(inode_id_t id) -> ChfsResult<FileAttr>;

  /**
   * Set the type of the inode
   */
  auto get_type(inode_id_t id) -> ChfsResult<InodeType>;

  /**
   * The combined version of the above APIs
   */
  auto get_type_attr(inode_id_t id)
      -> ChfsResult<std::pair<InodeType, FileAttr>>;

  auto get_reserved_blocks() const -> usize {
    return 1 + n_table_blocks + n_bitmap_blocks;
  }

  // helper functions

  /**
   * Set the block ID of the inode
   * @param idx: **physical** inode ID
   */
  auto set_table(inode_id_t idx, block_id_t bid) -> ChfsNullResult;

private:
  /**
   * Simple constructors
   */
  InodeManager(std::shared_ptr<BlockManager> bm, u64 max_inode_supported,
               u64 ntables, u64 nbit)
      : bm(bm), max_inode_supported(max_inode_supported),
        n_table_blocks(ntables), n_bitmap_blocks(nbit) {}

  /**
   * Read the inode to a buffer
   * @param block_id_t: the block id that stores the inode
   */
  auto read_inode(inode_id_t id, std::vector<u8> &buffer)
      -> ChfsResult<block_id_t>;
};

} // namespace chfs