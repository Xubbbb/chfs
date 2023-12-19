#include <vector>

#include "common/bitmap.h"
#include "metadata/inode.h"
#include "metadata/manager.h"
#include "distributed/commit_log.h"

namespace chfs {

/**
 * Transform a raw inode ID that index the table to a logic inode ID (and vice
 * verse) This prevents the inode ID with 0 to mix up with the invalid one
 */
#define RAW_2_LOGIC(i) (i + 1)
#define LOGIC_2_RAW(i) (i - 1)

InodeManager::InodeManager(std::shared_ptr<BlockManager> bm,
                           u64 max_inode_supported)
    : bm(bm) {
  // 1. calculate the number of bitmap blocks for the inodes
  auto inode_bits_per_block = bm->block_size() * KBitsPerByte;
  auto blocks_needed = max_inode_supported / inode_bits_per_block;

  // we align the bitmap to simplify bitmap calculations
  if (blocks_needed * inode_bits_per_block < max_inode_supported) {
    blocks_needed += 1;
  }
  this->n_bitmap_blocks = blocks_needed;

  // we may enlarge the max inode supported
  this->max_inode_supported = blocks_needed * KBitsPerByte * bm->block_size();

  // 2. initialize the inode table
  //...这里是默认一个inode只存储一个block id吗？似乎是inode table中存储每个inode的block id，真正的inode信息单独占据一个block, 比上课介绍的还要多一层...//
  auto inode_per_block = bm->block_size() / sizeof(block_id_t);
  auto table_blocks = this->max_inode_supported / inode_per_block;
  if (table_blocks * inode_per_block < this->max_inode_supported) {
    table_blocks += 1;
  }
  this->n_table_blocks = table_blocks;

  // 3. clear the bitmap blocks and table blocks
  for (u64 i = 0; i < this->n_table_blocks; ++i) {
    bm->zero_block(i + 1); // 1: the super block
  }

  for (u64 i = 0; i < this->n_bitmap_blocks; ++i) {
    bm->zero_block(i + 1 + this->n_table_blocks);
  }
}

auto InodeManager::create_from_block_manager(std::shared_ptr<BlockManager> bm,
                                             u64 max_inode_supported)
    -> ChfsResult<InodeManager> {
  auto inode_bits_per_block = bm->block_size() * KBitsPerByte;
  auto n_bitmap_blocks = max_inode_supported / inode_bits_per_block;

  CHFS_VERIFY(n_bitmap_blocks * inode_bits_per_block == max_inode_supported,
              "Wrong max_inode_supported");

  auto inode_per_block = bm->block_size() / sizeof(block_id_t);
  auto table_blocks = max_inode_supported / inode_per_block;
  if (table_blocks * inode_per_block < max_inode_supported) {
    table_blocks += 1;
  }

  InodeManager res = {bm, max_inode_supported, table_blocks, n_bitmap_blocks};
  return ChfsResult<InodeManager>(res);
}

// { Your code here }
auto InodeManager::allocate_inode(InodeType type, block_id_t bid)
    -> ChfsResult<inode_id_t> {
  auto iter_res = BlockIterator::create(this->bm.get(), 1 + n_table_blocks,
                                        1 + n_table_blocks + n_bitmap_blocks);
  if (iter_res.is_err()) {
    return ChfsResult<inode_id_t>(iter_res.unwrap_error());
  }

  inode_id_t count = 0;

  // Find an available inode ID.
  for (auto iter = iter_res.unwrap(); iter.has_next();
       iter.next(bm->block_size()).unwrap(), count++) {
    auto data = iter.unsafe_get_value_ptr<u8>();
    auto bitmap = Bitmap(data, bm->block_size());
    auto free_idx = bitmap.find_first_free();

    if (free_idx) {
      // If there is an available inode ID.

      // Setup the bitmap.
      bitmap.set(free_idx.value());
      auto res = iter.flush_cur_block();
      if (res.is_err()) {
        return ChfsResult<inode_id_t>(res.unwrap_error());
      }

      // TODO:
      // 1. Initialize the inode with the given type.
      // 2. Setup the inode table.
      // 3. Return the id of the allocated inode.
      //    You may have to use the `RAW_2_LOGIC` macro
      //    to get the result inode id.
      //UNIMPLEMENTED();

      // 在blockmanager中bid的block中初始化这个inode, 并且将inode写到bid这个block中, 完成初始化。
      auto new_inode = Inode(type, bm->block_size());
      std::vector<u8> buffer_indoe_block(bm->block_size());
      new_inode.flush_to_buffer(buffer_indoe_block.data());
      bm->write_block(bid, buffer_indoe_block.data());

      // 先计算该inode的index
      inode_id_t raw_inode_id = count * bm->block_size() * KBitsPerByte + free_idx.value();
      // 在inode table中找出记录(这一个inode的block id)的block和在这个block中的偏移量
      auto inode_per_block = bm->block_size() / sizeof(block_id_t);
      auto inode_table_block_id = 1 + (raw_inode_id / inode_per_block);
      // offset是在这个block中记录的第几个block id
      auto inode_table_block_offset = raw_inode_id % inode_per_block;

      //将inode table中的这个block读出
      std::vector<u8> buffer(bm->block_size());
      bm->read_block(inode_table_block_id, buffer.data()).unwrap();
      block_id_t * buffer_data_u64 = reinterpret_cast<block_id_t *>(buffer.data());
      buffer_data_u64[inode_table_block_offset] = bid;
      //将inode table 中的这个block写回
      bm->write_block(inode_table_block_id, reinterpret_cast<u8 *>(buffer_data_u64));

      return ChfsResult<inode_id_t>(RAW_2_LOGIC(raw_inode_id));
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::OUT_OF_RESOURCE);
}

//[ transaction ]//
// todo
auto InodeManager::allocate_inode_atomic(InodeType type, block_id_t bid, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) 
    -> ChfsResult<inode_id_t>{
  auto iter_res = BlockIterator::create(this->bm.get(), 1 + n_table_blocks,
                                        1 + n_table_blocks + n_bitmap_blocks);
  if (iter_res.is_err()) {
    return ChfsResult<inode_id_t>(iter_res.unwrap_error());
  }

  inode_id_t count = 0;
  block_id_t iter_block_id = 1 + n_table_blocks;
  //todo find available inode ID in memory
  // Find an available inode ID.
  for (auto iter = iter_res.unwrap(); iter.has_next();
       iter.next(bm->block_size()).unwrap(), count++, iter_block_id++) {
    bool is_bitmap_block_in_memory = false;
    auto bitmap_block_in_tx_ops_index = 0;
    auto data = iter.unsafe_get_value_ptr<u8>();
    auto bitmap = Bitmap(data, bm->block_size());
    std::vector<u8> new_block_state_buffer(bm->block_size());
    // traverse tx_ops to find if this bitmap block is in memory
    for(usize i=0;i<tx_ops.size();++i){
      if(tx_ops[i]->block_id_ == iter_block_id){
        is_bitmap_block_in_memory = true;
        bitmap_block_in_tx_ops_index = i;
      }
    }
    if(is_bitmap_block_in_memory){
      for(usize i=0;i<bm->block_size();++i){
        new_block_state_buffer[i] = tx_ops[bitmap_block_in_tx_ops_index]->new_block_state_[i];
      }
      bitmap = Bitmap(new_block_state_buffer.data(), bm->block_size());
    }
    auto free_idx = bitmap.find_first_free();

    if (free_idx) {
      // If there is an available inode ID.

      // Setup the bitmap.
      bitmap.set(free_idx.value());

      if(is_bitmap_block_in_memory){
        bm->write_block_to_memory(iter_block_id, new_block_state_buffer.data(), tx_ops);
      }
      else{
        auto res = iter.flush_cur_block_atomic(tx_ops);
        if (res.is_err()) {
          return ChfsResult<inode_id_t>(res.unwrap_error());
        }
      }

      // TODO:
      // 1. Initialize the inode with the given type.
      // 2. Setup the inode table.
      // 3. Return the id of the allocated inode.
      //    You may have to use the `RAW_2_LOGIC` macro
      //    to get the result inode id.
      //UNIMPLEMENTED();

      // 在blockmanager中bid的block中初始化这个inode, 并且将inode写到bid这个block中, 完成初始化。
      auto new_inode = Inode(type, bm->block_size());
      std::vector<u8> buffer_indoe_block(bm->block_size());
      new_inode.flush_to_buffer(buffer_indoe_block.data());
      bm->write_block_to_memory(bid, buffer_indoe_block.data(), tx_ops);

      // 先计算该inode的index
      inode_id_t raw_inode_id = count * bm->block_size() * KBitsPerByte + free_idx.value();
      // 在inode table中找出记录(这一个inode的block id)的block和在这个block中的偏移量
      auto inode_per_block = bm->block_size() / sizeof(block_id_t);
      auto inode_table_block_id = 1 + (raw_inode_id / inode_per_block);
      // offset是在这个block中记录的第几个block id
      auto inode_table_block_offset = raw_inode_id % inode_per_block;

      //将inode table中的这个block读出
      std::vector<u8> buffer(bm->block_size());
      bm->read_block_from_memory(inode_table_block_id, buffer.data(), tx_ops).unwrap();
      block_id_t * buffer_data_u64 = reinterpret_cast<block_id_t *>(buffer.data());
      buffer_data_u64[inode_table_block_offset] = bid;
      //将inode table 中的这个block写回
      bm->write_block_to_memory(inode_table_block_id, reinterpret_cast<u8 *>(buffer_data_u64), tx_ops);

      return ChfsResult<inode_id_t>(RAW_2_LOGIC(raw_inode_id));
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::OUT_OF_RESOURCE);
}
//[ transaction ]//

// { Your code here }
auto InodeManager::set_table(inode_id_t idx, block_id_t bid) -> ChfsNullResult {

  // TODO: Implement this function.
  // Fill `bid` into the inode table entry
  // whose index is `idx`.
  // UNIMPLEMENTED();

  auto inode_per_block = bm->block_size() / sizeof(block_id_t);
  auto inode_table_block_id = 1 + (idx / inode_per_block);
  // offset是在这个block中记录的第几个block id
  auto inode_table_block_offset = idx % inode_per_block;

  std::vector<u8> buffer(bm->block_size());
  bm->read_block(inode_table_block_id, buffer.data()).unwrap();
  block_id_t * buffer_data_u64 = reinterpret_cast<block_id_t *>(buffer.data());
  buffer_data_u64[inode_table_block_offset] = bid;
  bm->write_block(inode_table_block_id, reinterpret_cast<u8 *>(buffer_data_u64));

  return KNullOk;
}

//[ transaction ]//
auto InodeManager::set_table_atomic(inode_id_t idx, block_id_t bid, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsNullResult {

  // TODO: Implement this function.
  // Fill `bid` into the inode table entry
  // whose index is `idx`.
  // UNIMPLEMENTED();

  auto inode_per_block = bm->block_size() / sizeof(block_id_t);
  auto inode_table_block_id = 1 + (idx / inode_per_block);
  // offset是在这个block中记录的第几个block id
  auto inode_table_block_offset = idx % inode_per_block;

  std::vector<u8> buffer(bm->block_size());
  bm->read_block_from_memory(inode_table_block_id, buffer.data(), tx_ops).unwrap();
  block_id_t * buffer_data_u64 = reinterpret_cast<block_id_t *>(buffer.data());
  buffer_data_u64[inode_table_block_offset] = bid;
  bm->write_block_to_memory(inode_table_block_id, reinterpret_cast<u8 *>(buffer_data_u64), tx_ops);

  return KNullOk;
}
//[ transaction ]//

// { Your code here }
auto InodeManager::get(inode_id_t id) -> ChfsResult<block_id_t> {
  block_id_t res_block_id = 0;

  // TODO: Implement this function.
  // Get the block id of inode whose id is `id`
  // from the inode table. You may have to use
  // the macro `LOGIC_2_RAW` to get the inode
  // table index.
  //UNIMPLEMENTED();
  if(id == KInvalidInodeID){
    return ChfsResult<block_id_t>(ErrorType::INVALID_ARG);
  }
  inode_id_t inode_id_raw = LOGIC_2_RAW(id);
  // 在inode table中找出记录(这一个inode的block id)的block和在这个block中的偏移量
  auto inode_per_block = bm->block_size() / sizeof(block_id_t);
  auto inode_table_block_id = 1 + (inode_id_raw / inode_per_block);
  // offset是在这个block中记录的第几个block id
  auto inode_table_block_offset = inode_id_raw % inode_per_block;

  //将inode table中的这个block读出
  std::vector<u8> buffer(bm->block_size());
  bm->read_block(inode_table_block_id, buffer.data()).unwrap();
  block_id_t * buffer_data_u64 = reinterpret_cast<block_id_t *>(buffer.data());
  res_block_id = buffer_data_u64[inode_table_block_offset];

  return ChfsResult<block_id_t>(res_block_id);
}

// [transaction] //
auto InodeManager::get_from_memory(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsResult<block_id_t> {
  block_id_t res_block_id = 0;

  // TODO: Implement this function.
  // Get the block id of inode whose id is `id`
  // from the inode table. You may have to use
  // the macro `LOGIC_2_RAW` to get the inode
  // table index.
  //UNIMPLEMENTED();
  if(id == KInvalidInodeID){
    return ChfsResult<block_id_t>(ErrorType::INVALID_ARG);
  }
  inode_id_t inode_id_raw = LOGIC_2_RAW(id);
  // 在inode table中找出记录(这一个inode的block id)的block和在这个block中的偏移量
  auto inode_per_block = bm->block_size() / sizeof(block_id_t);
  auto inode_table_block_id = 1 + (inode_id_raw / inode_per_block);
  // offset是在这个block中记录的第几个block id
  auto inode_table_block_offset = inode_id_raw % inode_per_block;

  //将inode table中的这个block读出
  std::vector<u8> buffer(bm->block_size());
  bm->read_block_from_memory(inode_table_block_id, buffer.data(), tx_ops).unwrap();
  block_id_t * buffer_data_u64 = reinterpret_cast<block_id_t *>(buffer.data());
  res_block_id = buffer_data_u64[inode_table_block_offset];

  return ChfsResult<block_id_t>(res_block_id);
}
// [transaction] //

auto InodeManager::free_inode_cnt() const -> ChfsResult<u64> {
  auto iter_res = BlockIterator::create(this->bm.get(), 1 + n_table_blocks,
                                        1 + n_table_blocks + n_bitmap_blocks);

  if (iter_res.is_err()) {
    return ChfsResult<u64>(iter_res.unwrap_error());
  }

  u64 count = 0;
  for (auto iter = iter_res.unwrap(); iter.has_next();) {
    auto data = iter.unsafe_get_value_ptr<u8>();
    auto bitmap = Bitmap(data, bm->block_size());

    count += bitmap.count_zeros();

    auto iter_res = iter.next(bm->block_size());
    if (iter_res.is_err()) {
      return ChfsResult<u64>(iter_res.unwrap_error());
    }
  }
  return ChfsResult<u64>(count);
}

auto InodeManager::get_attr(inode_id_t id) -> ChfsResult<FileAttr> {
  std::vector<u8> buffer(bm->block_size());
  auto res = this->read_inode(id, buffer);
  if (res.is_err()) {
    return ChfsResult<FileAttr>(res.unwrap_error());
  }
  Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
  return ChfsResult<FileAttr>(inode_p->inner_attr);
}

auto InodeManager::get_type(inode_id_t id) -> ChfsResult<InodeType> {
  std::vector<u8> buffer(bm->block_size());
  auto res = this->read_inode(id, buffer);
  if (res.is_err()) {
    return ChfsResult<InodeType>(res.unwrap_error());
  }
  Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
  return ChfsResult<InodeType>(inode_p->type);
}

// [transaction] //
auto InodeManager::get_type_from_memory(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsResult<InodeType> {
  std::vector<u8> buffer(bm->block_size());
  auto res = this->read_inode_from_memory(id, buffer, tx_ops);
  if (res.is_err()) {
    return ChfsResult<InodeType>(res.unwrap_error());
  }
  Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
  return ChfsResult<InodeType>(inode_p->type);
}
// [transaction] //

auto InodeManager::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  std::vector<u8> buffer(bm->block_size());
  auto res = this->read_inode(id, buffer);
  if (res.is_err()) {
    return ChfsResult<std::pair<InodeType, FileAttr>>(res.unwrap_error());
  }
  Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
  return ChfsResult<std::pair<InodeType, FileAttr>>(
      std::make_pair(inode_p->type, inode_p->inner_attr));
}

// Note: the buffer must be as large as block size
auto InodeManager::read_inode(inode_id_t id, std::vector<u8> &buffer)
    -> ChfsResult<block_id_t> {
  if (id >= max_inode_supported - 1) {
    return ChfsResult<block_id_t>(ErrorType::INVALID_ARG);
  }

  auto block_id = this->get(id);
  if (block_id.is_err()) {
    return ChfsResult<block_id_t>(block_id.unwrap_error());
  }

  if (block_id.unwrap() == KInvalidBlockID) {
    return ChfsResult<block_id_t>(ErrorType::INVALID_ARG);
  }

  auto res = bm->read_block(block_id.unwrap(), buffer.data());
  if (res.is_err()) {
    return ChfsResult<block_id_t>(res.unwrap_error());
  }
  return ChfsResult<block_id_t>(block_id.unwrap());
}

// [transaction] //
auto InodeManager::read_inode_from_memory(inode_id_t id, std::vector<u8> &buffer, std::vector<std::shared_ptr<BlockOperation>> &tx_ops)
    -> ChfsResult<block_id_t> {
  if (id >= max_inode_supported - 1) {
    return ChfsResult<block_id_t>(ErrorType::INVALID_ARG);
  }

  auto block_id = this->get_from_memory(id, tx_ops);
  if (block_id.is_err()) {
    return ChfsResult<block_id_t>(block_id.unwrap_error());
  }

  if (block_id.unwrap() == KInvalidBlockID) {
    return ChfsResult<block_id_t>(ErrorType::INVALID_ARG);
  }

  auto res = bm->read_block_from_memory(block_id.unwrap(), buffer.data(), tx_ops);
  if (res.is_err()) {
    return ChfsResult<block_id_t>(res.unwrap_error());
  }
  return ChfsResult<block_id_t>(block_id.unwrap());
}
// [transaction] //

// {Your code}
auto InodeManager::free_inode(inode_id_t id) -> ChfsNullResult {

  // simple pre-checks
  if (id >= max_inode_supported - 1) {
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }

  // TODO:
  // 1. Clear the inode table entry.
  //    You may have to use macro `LOGIC_2_RAW`
  //    to get the index of inode table from `id`.
  // 2. Clear the inode bitmap.
  //UNIMPLEMENTED();

  inode_id_t idx = LOGIC_2_RAW(id);
  this->set_table(idx, KInvalidBlockID);

  inode_id_t bitmap_block_offset = idx / (bm->block_size() * KBitsPerByte);
  auto bitmap_bit_offset = idx % (bm->block_size() * KBitsPerByte);
  std::vector<u8> buffer(bm->block_size());
  bm->read_block(1 + n_table_blocks + bitmap_block_offset, buffer.data());
  auto bitmap = Bitmap(buffer.data(), bm->block_size());
  bitmap.clear(bitmap_bit_offset);
  bm->write_block(1 + n_table_blocks + bitmap_block_offset, buffer.data());

  return KNullOk;
}

//[ transaction ]//
auto InodeManager::free_inode_atomic(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsNullResult{
  // simple pre-checks
  if (id >= max_inode_supported - 1) {
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }

  // TODO:
  // 1. Clear the inode table entry.
  //    You may have to use macro `LOGIC_2_RAW`
  //    to get the index of inode table from `id`.
  // 2. Clear the inode bitmap.
  //UNIMPLEMENTED();

  inode_id_t idx = LOGIC_2_RAW(id);
  this->set_table_atomic(idx, KInvalidBlockID, tx_ops);

  inode_id_t bitmap_block_offset = idx / (bm->block_size() * KBitsPerByte);
  auto bitmap_bit_offset = idx % (bm->block_size() * KBitsPerByte);
  std::vector<u8> buffer(bm->block_size());
  bm->read_block_from_memory(1 + n_table_blocks + bitmap_block_offset, buffer.data(), tx_ops);
  auto bitmap = Bitmap(buffer.data(), bm->block_size());
  bitmap.clear(bitmap_bit_offset);
  bm->write_block_to_memory(1 + n_table_blocks + bitmap_block_offset, buffer.data(), tx_ops);

  return KNullOk;
}
//[ transaction ]//

} // namespace chfs