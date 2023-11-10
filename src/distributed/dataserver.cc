#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  //! change the bitmap offset in bm, reserve some blocks as version blocks
  auto num_of_version_block = (KDefaultBlockCnt * sizeof(version_t)) / bm->block_size();
  if (is_initialized) {
    block_allocator_ =
        // std::make_shared<BlockAllocator>(bm, 0, false);
        std::make_shared<BlockAllocator>(bm, num_of_version_block, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        // new BlockAllocator(bm, 0, true));
        new BlockAllocator(bm, num_of_version_block, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  const auto BLOCK_SIZE = block_allocator_->bm->block_size();
  std::vector<u8> res_buf(0);
  // 首先判断读取的内容是否合法, 若block_id过大，或者读取的内容超出了一个block属于invalid读取
  if(block_id >= block_allocator_->bm->total_blocks() || (offset + len > BLOCK_SIZE)){
    return res_buf;
  }
  std::vector<u8> entire_block_buffer(BLOCK_SIZE);
  auto read_res = block_allocator_->bm->read_block(block_id, entire_block_buffer.data());
  if(read_res.is_err()){
    return res_buf;
  }

  //...check the version is correct or not...//
  // get the version 
  auto KVersionPerBlock = BLOCK_SIZE / sizeof(version_t);
  auto version_block_id = block_id / KVersionPerBlock;
  auto version_in_block_idx = block_id % KVersionPerBlock;
  std::vector<u8> version_buf(BLOCK_SIZE);
  auto read_version_block_res = block_allocator_->bm->read_block(version_block_id, version_buf.data());
  if(read_version_block_res.is_err()){
    return res_buf;
  }
  auto version_arr = reinterpret_cast<version_t *>(version_buf.data());
  auto local_version = version_arr[version_in_block_idx];
  if(local_version != version){
    return res_buf;
  }
  //...check the version finished...//

  res_buf.resize(len);
  for(usize i=0;i < len;++i){
    res_buf[i] = entire_block_buffer[offset + i];
  }
  return res_buf;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  const auto BLOCK_SIZE = block_allocator_->bm->block_size();
  usize len = buffer.size();
  if(block_id >= block_allocator_->bm->total_blocks() || (offset + len > BLOCK_SIZE)){
    return false;
  }
  // TODO maybe: check the block is valid
  auto write_res = block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, len);
  if(write_res.is_err()){
    return false;
  }
  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  const auto BLOCK_SIZE = block_allocator_->bm->block_size();
  auto allocate_res = block_allocator_->allocate();
  if(allocate_res.is_err()){
    return std::pair<block_id_t, version_t>(0,0);
  }
  block_id_t res_block_id = allocate_res.unwrap();
  //...update the local version...//
  auto KVersionPerBlock = BLOCK_SIZE / sizeof(version_t);
  auto version_block_id = res_block_id / KVersionPerBlock;
  auto version_in_block_idx = res_block_id % KVersionPerBlock;
  std::vector<u8> version_buf(BLOCK_SIZE);
  auto read_version_block_res = block_allocator_->bm->read_block(version_block_id, version_buf.data());
  if(read_version_block_res.is_err()){
    return std::pair<block_id_t, version_t>(0,0);
  }
  auto version_arr = reinterpret_cast<version_t *>(version_buf.data());
  auto new_version = version_arr[version_in_block_idx] + 1;
  version_arr[version_in_block_idx] = new_version;
  auto write_version_block_res = block_allocator_->bm->write_block(version_block_id, version_buf.data());
  if(write_version_block_res.is_err()){
    return std::pair<block_id_t, version_t>(res_block_id, new_version - 1);
  }
  //...update finish...//
  std::pair<block_id_t, version_t> res(res_block_id, new_version);
  return res;
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  const auto BLOCK_SIZE = block_allocator_->bm->block_size();
  auto deallocate_res = block_allocator_->deallocate(block_id);
  if(deallocate_res.is_err()){
    return false;
  }
  //...update the local version...//
  auto KVersionPerBlock = BLOCK_SIZE / sizeof(version_t);
  auto version_block_id = block_id / KVersionPerBlock;
  auto version_in_block_idx = block_id % KVersionPerBlock;
  std::vector<u8> version_buf(BLOCK_SIZE);
  auto read_version_block_res = block_allocator_->bm->read_block(version_block_id, version_buf.data());
  if(read_version_block_res.is_err()){
    return false;
  }
  auto version_arr = reinterpret_cast<version_t *>(version_buf.data());
  auto new_version = version_arr[version_in_block_idx] + 1;
  version_arr[version_in_block_idx] = new_version;
  auto write_version_block_res = block_allocator_->bm->write_block(version_block_id, version_buf.data());
  if(write_version_block_res.is_err()){
    return false;
  }
  //...update finish...//
  return true;
}
} // namespace chfs