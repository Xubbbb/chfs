//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// commit_log.h
//
// Identification: src/include/distributed/commit_log.h
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "block/manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "filesystem/operations.h"
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace chfs {
/**
 * `BlockOperation` is an entry indicates an old block state and
 * a new block state. It's used to redo the operation when
 * the system is crashed.
 */

class BlockOperation {
public:
  explicit BlockOperation(block_id_t block_id, std::vector<u8> new_block_state)
      : block_id_(block_id), new_block_state_(new_block_state) {
    CHFS_ASSERT(new_block_state.size() == DiskBlockSize, "invalid block state");
  }

  block_id_t block_id_;
  std::vector<u8> new_block_state_;
};

class LogEntry{
public:
  txn_id_t txn_id;
  block_id_t block_id;
  u8 new_block_state[0];

  // attention: this function is only used for constructing a LogEntry
  auto flush_to_buffer(u8 *buffer) const {
    auto log_entry_ptr = reinterpret_cast<LogEntry *>(buffer);
    log_entry_ptr->txn_id = txn_id;
    log_entry_ptr->block_id = block_id;
    for (int i = 0; i < DiskBlockSize; ++i) {
      log_entry_ptr->new_block_state[i] = 0;
    }
  }
} __attribute__((packed));


/**
 * `CommitLog` is a class that records the block edits into the
 * commit log. It's used to redo the operation when the system
 * is crashed.
 */
class CommitLog {
public:
  explicit CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled);
  ~CommitLog();
  auto append_log(txn_id_t txn_id,
                  std::vector<std::shared_ptr<BlockOperation>> ops) -> void;
  auto commit_log(txn_id_t txn_id) -> void;
  auto checkpoint() -> void;
  auto recover() -> void;
  auto get_log_entry_num() -> usize;

  bool is_checkpoint_enabled_;
  std::shared_ptr<BlockManager> bm_;
  /**
   * {Append anything if you need}
   */
  u64 log_current_offset;
  txn_id_t num_tx_in_log;
  txn_id_t current_txn_id;
  std::mutex log_mtx;

  auto generate_txn_id() -> txn_id_t{
    //! log_mtx //
    log_mtx.lock();
    //! log_mtx //
    txn_id_t tmp = current_txn_id;
    ++current_txn_id;
    if(current_txn_id == 0){
      current_txn_id = 1;
    }
    //! log_mtx //
    log_mtx.unlock();
    //! log_mtx //
    return tmp;
  }
};

} // namespace chfs