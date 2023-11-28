//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// allocator.h
//
// Identification: src/include/block/allocator.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "block/manager.h"

namespace chfs {

class SuperBlock;
class InodeManager;

/**
 * BlockManager implements a block allocator to manage blocks of the manager
 * It internally uses bitmap for the management.
 * Note that the block allocator is **not** thread-safe. In lab2, you should make
 * it thread-safe.
 *
 * # Example
 *
 * TBD
 */
class BlockAllocator {
  friend class SuperBlock;
  friend class nodeManager;

public:
  std::shared_ptr<BlockManager> bm;

protected:
  // The bitmap block is stored at [bitmap_block_id, bitmap_block_id +
  // bitmap_block_cnt - 1]
  block_id_t bitmap_block_id;
  block_id_t bitmap_block_cnt;

  // number of bits needed in the last bitmap block
  usize last_block_num;

public:
  /**
   * Creates a new block allocator with a block manager.
   * The first block is used to store the bitmap.
   *
   * @param bm the block manager
   */
  explicit BlockAllocator(std::shared_ptr<BlockManager> bm);

  /**
   *  Creates a new block allocator with a block manager and a bitmap block id.
   *
   * @param bm the block manager
   * @param bitmap_block_id the block id of the bitmap
   * @param will_initialize whether to initialize the bitmap
   *
   * # Note!!!!
   * We assume that the blocks before the `bitmap_block_id` is reserved,
   * so they cannot be allocated.
   */
  BlockAllocator(std::shared_ptr<BlockManager> bm, usize bitmap_block_id,
                 bool will_initialize = true);

  auto total_bitmap_block() -> usize { return this->bitmap_block_cnt; }

  /**
   * Count the number of free blocks.
   *
   * @return the number of free blocks
   */
  auto free_block_cnt() const -> usize;

  /**
   * Allocate a block.
   *
   * @return the block id of the allocated block if succeed.
   *         OUT_OF_RESOURCE if there is no free block.
   *         other error code if there is other error.
   */
  auto allocate() -> ChfsResult<block_id_t>;

  /**
   * Deallocate a block.
   * @param block_id the block id to be deallocated.
   *
   * @return INVALID_ARG if the block id is freed.
   *         other error code if there is other error.
   */
  auto deallocate(block_id_t block_id) -> ChfsNullResult;


  //[ transaction ]//
  /**
   * Allocate a block atomically
   * 
   * @param tx_ops vector to store all block operation
   *
   * @return the block id of the allocated block if succeed.
   *         OUT_OF_RESOURCE if there is no free block.
   *         other error code if there is other error.
   */
  auto allocate_atomic(std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsResult<block_id_t>;

  /**
   * Deallocate a block.
   * @param block_id the block id to be deallocated.
   * @param tx_ops vector to store all block operation
   *
   * @return INVALID_ARG if the block id is freed.
   *         other error code if there is other error.
   */
  auto deallocate_atomic(block_id_t block_id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsNullResult;

  //[ transaction ]//
};

} // namespace chfs