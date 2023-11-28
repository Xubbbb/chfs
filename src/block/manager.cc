#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "distributed/commit_log.h"
// #include "block/manager.h"

namespace chfs {

auto get_file_sz(std::string &file_name) -> usize {
  std::filesystem::path path = file_name;
  return std::filesystem::file_size(path);
}

/**
 * If the opened block manager's backed file is empty,
 * we will initialize it to a pre-defined size.
 */
auto initialize_file(int fd, u64 total_file_sz) {
  if (ftruncate(fd, total_file_sz) == -1) {
    CHFS_ASSERT(false, "Failed to initialize the block manager file");
  }
}

/**
 * Constructor: open/create a single database file & log file
 * @input db_file: database file name
 */
BlockManager::BlockManager(const std::string &file)
    : BlockManager(file, KDefaultBlockCnt) {}

/**
 * Creates a new block manager that writes to a file-backed block device.
 * @param block_file the file name of the  file to write to
 * @param block_cnt the number of expected blocks in the device. If the
 * device's blocks are more or less than it, the manager should adjust the
 * actual block cnt.
 */
BlockManager::BlockManager(usize block_cnt, usize block_size)
    : block_sz(block_size), file_name_("in-memory"), fd(-1),
      block_cnt(block_cnt), in_memory(true), is_log_enabled(false) {
  // An important step to prevent overflow
  this->write_fail_cnt = 0;
  this->maybe_failed = false;
  u64 buf_sz = static_cast<u64>(block_cnt) * static_cast<u64>(block_size);
  CHFS_VERIFY(buf_sz > 0, "Santiy check buffer size fails");
  this->block_data = new u8[buf_sz];
  CHFS_VERIFY(this->block_data != nullptr, "Failed to allocate memory");
}

/**
 * Core constructor: open/create a single database file & log file
 * @input db_file: database file name
 */
BlockManager::BlockManager(const std::string &file, usize block_cnt)
    : file_name_(file), block_cnt(block_cnt), in_memory(false), is_log_enabled(false) {
  this->write_fail_cnt = 0;
  this->maybe_failed = false;
  this->fd = open(file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  CHFS_ASSERT(this->fd != -1, "Failed to open the block manager file");

  auto file_sz = get_file_sz(this->file_name_);
  if (file_sz == 0) {
    initialize_file(this->fd, this->total_storage_sz());
  } else {
    this->block_cnt = file_sz / this->block_sz;
    CHFS_ASSERT(this->total_storage_sz() == KDefaultBlockCnt * this->block_sz,
                "The file size mismatches");
  }

  this->block_data =
      static_cast<u8 *>(mmap(nullptr, this->total_storage_sz(),
                             PROT_READ | PROT_WRITE, MAP_SHARED, this->fd, 0));
  CHFS_ASSERT(this->block_data != MAP_FAILED, "Failed to mmap the data");
}

BlockManager::BlockManager(const std::string &file, usize block_cnt, bool is_log_enabled)
    : file_name_(file), block_cnt(block_cnt), in_memory(false), is_log_enabled(is_log_enabled) {
  this->write_fail_cnt = 0;
  this->maybe_failed = false;
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  // enable log, we need to reserve 1024 blocks， we will put these blocks at the end of block arr and we will decrease our block_cnt
  const auto LOG_BLOCK_NUM = 1024;
  this->fd = open(file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  CHFS_ASSERT(this->fd != -1, "Failed to open the block manager file");

  auto file_sz = get_file_sz(this->file_name_);
  if (file_sz == 0) {
    initialize_file(this->fd, this->total_storage_sz());
  } else {
    this->block_cnt = file_sz / this->block_sz;
    CHFS_ASSERT(this->total_storage_sz() == KDefaultBlockCnt * this->block_sz,
                "The file size mismatches");
  }

  this->block_data =
      static_cast<u8 *>(mmap(nullptr, this->total_storage_sz(),
                             PROT_READ | PROT_WRITE, MAP_SHARED, this->fd, 0));
  CHFS_ASSERT(this->block_data != MAP_FAILED, "Failed to mmap the data");

  if(is_log_enabled){
    CHFS_ASSERT(this->block_cnt > LOG_BLOCK_NUM, "there is not enough blocks to enable log feature");
  } 
}

auto BlockManager::write_block(block_id_t block_id, const u8 *data)
    -> ChfsNullResult {
  if (this->maybe_failed && block_id < this->block_cnt) {
    if (this->write_fail_cnt >= 3) {
      this->write_fail_cnt = 0;
      return ErrorType::INVALID;
    }
  }
  

  // TODO: Implement this function.
  //UNIMPLEMENTED();
  if(block_id >= block_cnt){
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }
  auto block_offset = block_id * this->block_sz;
  for(usize i=0;i<this->block_sz;++i){
    block_data[block_offset + i] = data[i];
  }
  this->write_fail_cnt++;
  return KNullOk;
}

//[ transaction ]//
auto BlockManager::write_block_to_memory(block_id_t block_id, const u8 *data, std::vector<std::shared_ptr<BlockOperation>> &tx_ops)
      -> ChfsNullResult{
  if(block_id >= block_cnt){
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }
  auto block_offset = block_id * this->block_sz;
  bool is_changed = false;
  for(usize i=0;i < this->block_sz;++i){
    if(block_data[block_offset + i] != data[i]){
      is_changed = true;
      break;
    }
  }
  if(is_changed){
    std::shared_ptr<BlockOperation> new_op;
    std::vector<u8> new_block_state(data, data + this->block_sz);
    new_op = std::make_shared<BlockOperation>(block_id, new_block_state);
    tx_ops.push_back(new_op);
  }
  return KNullOk;
}

auto BlockManager::read_block_from_memory(block_id_t block_id, u8 *data, std::vector<std::shared_ptr<BlockOperation>> &tx_ops)
      -> ChfsNullResult{
  if(block_id >= block_cnt){
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }
  // find the newest block state
  bool is_in_tx_ops = false;
  usize newest_op_idx = 0;
  for(usize i=0;i<tx_ops.size();++i){
    if(tx_ops[i]->block_id_ == block_id){
      is_in_tx_ops = true;
      newest_op_idx = i;
    }
  }
  if(is_in_tx_ops){
    for(usize i=0;i<this->block_sz;++i){
      data[i] = tx_ops[newest_op_idx]->new_block_state_[i];
    }
    return KNullOk;
  }
  // else we need to read from block_data
  auto block_offset = block_id * this->block_sz;
  for(usize i=0;i<this->block_sz;++i){
    data[i] = block_data[block_offset + i];
  }
  return KNullOk;
}

auto BlockManager::write_log_entry(usize offset, const u8 *data, usize len) -> ChfsNullResult{
  const auto base_offset = (block_cnt - 1024) * block_sz;
  for(usize i = 0;i < len;++i){
    block_data[base_offset + offset + i] = data[i];
  }
  return KNullOk;
}

auto BlockManager::write_block_for_recover(block_id_t block_id, const u8 *data)
    -> ChfsNullResult {
  if(block_id >= block_cnt){
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }
  auto block_offset = block_id * this->block_sz;
  for(usize i=0;i<this->block_sz;++i){
    block_data[block_offset + i] = data[i];
  }
  return KNullOk;
}

//[ transaction ]//

auto BlockManager::write_partial_block(block_id_t block_id, const u8 *data,
                                       usize offset, usize len)
    -> ChfsNullResult {
  if (this->maybe_failed && block_id < this->block_cnt) {
    if (this->write_fail_cnt >= 3) {
      this->write_fail_cnt = 0;
      return ErrorType::INVALID;
    }
  }

  // TODO: Implement this function.
  //UNIMPLEMENTED();
  if(block_id >= block_cnt){
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }
  auto block_offset = block_id * this->block_sz;

  for(usize i=0;i<len;++i){
    block_data[block_offset + offset + i] = data[i];
  }
  this->write_fail_cnt++;
  return KNullOk;
}

auto BlockManager::read_block(block_id_t block_id, u8 *data) -> ChfsNullResult {

  // TODO: Implement this function.
  //UNIMPLEMENTED();
  if(block_id >= block_cnt){
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }
  auto block_offset = block_id * this->block_sz;
  for(usize i=0;i<this->block_sz;++i){
    data[i] = block_data[block_offset + i];
  }
  return KNullOk;
}

auto BlockManager::zero_block(block_id_t block_id) -> ChfsNullResult {
  
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  usize block_offset = block_id * this->block_sz;
  for(usize i=0;i<this->block_sz;++i){
    block_data[block_offset + i] = (u8)0;
  }

  return KNullOk;
}

auto BlockManager::sync(block_id_t block_id) -> ChfsNullResult {
  if (block_id >= this->block_cnt) {
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }

  auto res = msync(this->block_data + block_id * this->block_sz, this->block_sz,
        MS_SYNC | MS_INVALIDATE);
  if (res != 0)
    return ChfsNullResult(ErrorType::INVALID);
  return KNullOk;
}

auto BlockManager::flush() -> ChfsNullResult {
  auto res = msync(this->block_data, this->block_sz * this->block_cnt, MS_SYNC | MS_INVALIDATE);
  if (res != 0)
    return ChfsNullResult(ErrorType::INVALID);
  return KNullOk;
}

BlockManager::~BlockManager() {
  if (!this->in_memory) {
    munmap(this->block_data, this->total_storage_sz());
    close(this->fd);
  } else {
    delete[] this->block_data;
  }
}

// BlockIterator
auto BlockIterator::create(BlockManager *bm, block_id_t start_block_id,
                           block_id_t end_block_id)
    -> ChfsResult<BlockIterator> {
  BlockIterator iter;
  iter.bm = bm;
  iter.cur_block_off = 0;
  iter.start_block_id = start_block_id;
  iter.end_block_id = end_block_id;

  std::vector<u8> buffer(bm->block_sz);

  auto res = bm->read_block(iter.cur_block_off / bm->block_sz + start_block_id,
                            buffer.data());
  if (res.is_ok()) {
    iter.buffer = std::move(buffer);
    return ChfsResult<BlockIterator>(iter);
  }
  return ChfsResult<BlockIterator>(res.unwrap_error());
}

// assumption: a previous call of has_next() returns true
auto BlockIterator::next(usize offset) -> ChfsNullResult {
  auto prev_block_id = this->cur_block_off / bm->block_size();
  this->cur_block_off += offset;

  auto new_block_id = this->cur_block_off / bm->block_size();
  // move forward
  if (new_block_id != prev_block_id) {
    if (this->start_block_id + new_block_id > this->end_block_id) {
      return ChfsNullResult(ErrorType::DONE);
    }

    // else: we need to refresh the buffer
    auto res = bm->read_block(this->start_block_id + new_block_id,
                              this->buffer.data());
    if (res.is_err()) {
      return ChfsNullResult(res.unwrap_error());
    }
  }
  return KNullOk;
}

} // namespace chfs
