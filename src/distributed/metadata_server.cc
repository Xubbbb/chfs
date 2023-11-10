#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  //! global mutex !//
  global_mtx.lock();
  //! global mutex !//
  if(type == DirectoryType){
    auto mkdir_res = operation_->mkdir(parent, name.data());
    if(mkdir_res.is_err()){
      //! global mutex !//
      global_mtx.unlock();
      //! global mutex !//
      return KInvalidInodeID;
    }
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return mkdir_res.unwrap();
  }
  else if(type == RegularFileType){
    auto mkfile_res = operation_->mkfile(parent, name.data());
    if(mkfile_res.is_err()){
      //! global mutex !//
      global_mtx.unlock();
      //! global mutex !//
      return KInvalidInodeID;
    }
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return mkfile_res.unwrap();
  }
  else{
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return KInvalidInodeID;
  }
  //! global mutex !//
  global_mtx.unlock();
  //! global mutex !//
  return 0;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  //! global mutex !//
  global_mtx.lock();
  //! global mutex !//
  // 由于unlink一个file的时候会涉及到多个节点上存储的block的deallocate, 故需要重写
  auto lookup_res = operation_->lookup(parent, name.data());
  if(lookup_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  auto unlink_inode_id = lookup_res.unwrap();
  auto type_res = operation_->gettype(unlink_inode_id);
  if(type_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  auto unlink_type = type_res.unwrap();
  if(unlink_type != InodeType::FILE){
    if(unlink_type == InodeType::Directory){
      // for dir type, this is the same as lab1 unlink
      auto unlink_res = operation_->unlink(parent, name.data());
      if(unlink_res.is_err()){
        //! global mutex !//
        global_mtx.unlock();
        //! global mutex !//
        return false;
      }
      //! global mutex !//
      global_mtx.unlock();
      //! global mutex !//
      return true;
    }
    else{
      //! global mutex !//
      global_mtx.unlock();
      //! global mutex !//
      return false;
    }
  }
  // 下面实现remove_file的逻辑
  // 获取该文件中所有数据block的信息
  auto block_info_list = this->get_block_map(unlink_inode_id);
  // 获取存储inode信息的block的位置
  auto inode_block_res = operation_->inode_manager_->get(unlink_inode_id);
  if(inode_block_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  auto inode_block_id = inode_block_res.unwrap();
  // 首先free inode
  auto free_inode_res = operation_->inode_manager_->free_inode(unlink_inode_id);
  if(free_inode_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }

  // free所有相关的block
  auto local_free_res = operation_->block_allocator_->deallocate(inode_block_id);
  if(local_free_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  for(auto block_info : block_info_list){
    block_id_t block_id = std::get<0>(block_info);
    mac_id_t mac_id = std::get<1>(block_info);
    // version_t version_id = std::get<2>(block_info);
    auto it = clients_.find(mac_id);
    if(it == clients_.end()){
      // 没有找到这台机器
      //! global mutex !//
      global_mtx.unlock();
      //! global mutex !//
      return false;
    }
    else{
      auto target_mac = it->second;
      auto response = target_mac->call("free_block", block_id);
      if(response.is_err()){
        //! global mutex !//
        global_mtx.unlock();
        //! global mutex !//
        return false;
      }
      auto is_success = response.unwrap()->as<bool>();
      if(!is_success){
        //! global mutex !//
        global_mtx.unlock();
        //! global mutex !//
        return false;
      }
    }
  }

  // remove this file record from its parent directort entity
  auto read_parent_res = operation_->read_file(parent);
  if(read_parent_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  auto parent_content = read_parent_res.unwrap();
  std::string parent_content_str(reinterpret_cast<char *>(parent_content.data()), parent_content.size());
  std::string parent_content_str_change = rm_from_directory(parent_content_str, name);
  std::vector<u8> new_dir_vec(parent_content_str_change.begin(), parent_content_str_change.end());
  auto write_res = operation_->write_file(parent, new_dir_vec);
  if(write_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  //! global mutex !//
  global_mtx.unlock();
  //! global mutex !//
  return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  auto lookup_res = operation_->lookup(parent, name.data());
  if(lookup_res.is_err()){
    return KInvalidInodeID;
  }
  return lookup_res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  const auto BLOCK_SIZE = operation_->block_manager_->block_size();
  std::vector<u8> buffer(BLOCK_SIZE);
  std::vector<BlockInfo> res(0);

  if(id > operation_->inode_manager_->get_max_inode_supported()){
    return res;
  }
  auto type_res = operation_->gettype(id);
  if(type_res.is_err()){
    return res;
  }
  auto inode_type = type_res.unwrap();
  if(inode_type != InodeType::FILE){
    return res;
  }
  auto get_res = operation_->inode_manager_->get(id);
  if(get_res.is_err()){
    return res;
  }
  auto inode_block_id = get_res.unwrap();
  if(inode_block_id == KInvalidBlockID){
    return res;
  }
  auto read_block_res = operation_->block_manager_->read_block(inode_block_id, buffer.data());
  if(read_block_res.is_err()){
    return res;
  }
  Inode* inode_ptr = reinterpret_cast<Inode *>(buffer.data());
  // auto file_size = inode_ptr->get_size();

  // 直到读到第一个invalid的block id
  for(uint i = 0;i < inode_ptr->get_nblocks();i = i + 2){
    if(inode_ptr->blocks[i] == KInvalidBlockID){
      break;
    }
    block_id_t block_id = inode_ptr->blocks[i];
    mac_id_t mac_id = static_cast<mac_id_t>(inode_ptr->blocks[i + 1]);
    // TODO : add version id into map
    //...call data server to return version block to get the latest version of this block...//
    auto it = clients_.find(mac_id);
    if(it == clients_.end()){
      res.clear();
      return res;
    }
    auto target_mac = it->second;
    auto KVersionPerBlock = BLOCK_SIZE / sizeof(version_t);
    auto version_block_id = block_id / KVersionPerBlock;
    auto version_in_block_idx = block_id % KVersionPerBlock;
    //! for version block's version should be 0 forever
    auto response = target_mac->call("read_data", version_block_id, version_in_block_idx * sizeof(version_t), sizeof(version_t), 0);
    if(response.is_err()){
      res.clear();
      return res;
    }
    auto response_vec = response.unwrap()->as<std::vector<u8>>();
    auto version_ptr = reinterpret_cast<version_t *>(response_vec.data());
    //...fetch version finish...//
    version_t version_id = *version_ptr;
    res.push_back(BlockInfo(block_id, mac_id, version_id));
  }

  return res;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  //! global mutex !//
  global_mtx.lock();
  //! global mutex !//
  if(id > operation_->inode_manager_->get_max_inode_supported()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return BlockInfo(KInvalidBlockID, 0, 0);
  }
  // we assume that the file inode don't have indirect block and the block_id & mac_id are organized as this form in the inode block:

  // type | file_attr | block_id | mac_id | block_id | mac_id | ...  | block_id | mac_id (fortunatelly, block_id and mac_id is the same byte long); 
  
  // I don't know whether I should confirm this inode is a file inode, I assume that I don't need to confirm this is a file inode.

  // first, we will check whether the inode block have enough space to store a new block info pair
  const auto BLOCK_SIZE = operation_->block_manager_->block_size();
  usize old_block_num = 0;
  // u64 original_file_sz = 0;

  // 1. read the inode
  std::vector<u8> file_inode(BLOCK_SIZE);
  auto inode_ptr = reinterpret_cast<Inode *>(file_inode.data());
  auto get_res = operation_->inode_manager_->get(id);
  if(get_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return BlockInfo(KInvalidBlockID, 0, 0);
  }
  auto inode_block_id = get_res.unwrap();
  if(inode_block_id == KInvalidBlockID){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return BlockInfo(KInvalidBlockID, 0, 0);
  }
  auto read_block_res = operation_->block_manager_->read_block(inode_block_id, file_inode.data());
  if(read_block_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return BlockInfo(KInvalidBlockID, 0, 0);
  }
  // 2. make sure that we have space to allocate a new block for this file
  // it seems like it just only alloc not write, so this operation won't change attr ???
  // there are two methods to get old_block_num
  // (1) by file size
  // original_file_sz = inode_ptr->get_size();
  // old_block_num = (original_file_sz % BLOCK_SIZE) ? (original_file_sz / BLOCK_SIZE + 1) : (original_file_sz / BLOCK_SIZE);
  // (2) by first Invalid slot
  old_block_num = 0;
  for(uint i = 0;i < inode_ptr->get_nblocks();i = i + 2){
    if(inode_ptr->blocks[i] == KInvalidBlockID){
      old_block_num = i / 2;
      break;
    }
  }

  // check after alloc the Inode block is whether full or not
  if(2 * (old_block_num + 1) > inode_ptr->get_nblocks()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return BlockInfo(KInvalidBlockID, 0, 0);
  }
  auto idx = 2 * old_block_num;
  // prepare work finish

  // second, we randomly alloc a block from dataservers
  std::vector<mac_id_t> mac_ids;
  for (const auto& pair : clients_) {
    mac_ids.push_back(pair.first);
  }
  //! notice: this rand function is not as usually closed on the left, open on the right([a, b)), this is a both closed [a,b]
  auto rand_num = generator.rand(0, mac_ids.size() - 1);
  mac_id_t target_mac_id = mac_ids[rand_num];
  auto it = clients_.find(target_mac_id);
  if(it == clients_.end()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return BlockInfo(KInvalidBlockID, 0, 0);
  }
  auto target_mac = it->second;
  auto response = target_mac->call("alloc_block");
  if(response.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return BlockInfo(KInvalidBlockID, target_mac_id, 0);
  }
  auto response_pair = response.unwrap()->as<std::pair<chfs::block_id_t, chfs::version_t>>();
  auto block_id = response_pair.first;
  auto version_id = response_pair.second;
  
  // third, update the info in metadata_server locally
  inode_ptr->set_block_direct(idx, block_id);
  inode_ptr->set_block_direct(idx + 1, target_mac_id);
  
  // maybe todo: update the attr of inode

  auto write_res = operation_->block_manager_->write_block(inode_block_id, file_inode.data());
  if(write_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return BlockInfo(KInvalidBlockID, 0, 0);
  }
  //! global mutex !//
  global_mtx.unlock();
  //! global mutex !//
  return BlockInfo(block_id, target_mac_id, version_id);
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  //! global mutex !//
  global_mtx.lock();
  //! global mutex !//
  // first we find and check this record in inode block, whether the block id and machine id is right
  if(id > operation_->inode_manager_->get_max_inode_supported()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  if(block_id == KInvalidBlockID){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  const auto BLOCK_SIZE = operation_->block_manager_->block_size();

  // 1. read the inode
  std::vector<u8> file_inode(BLOCK_SIZE);
  auto inode_ptr = reinterpret_cast<Inode *>(file_inode.data());
  auto get_res = operation_->inode_manager_->get(id);
  if(get_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  auto inode_block_id = get_res.unwrap();
  if(inode_block_id == KInvalidBlockID){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  auto read_block_res = operation_->block_manager_->read_block(inode_block_id, file_inode.data());
  if(read_block_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  // find this record 
  bool is_found = false;
  uint record_idx = 0;
  for(uint i = 0;i < inode_ptr->get_nblocks();i = i + 2){
    if(inode_ptr->blocks[i] == block_id && inode_ptr->blocks[i + 1] == machine_id){
      is_found = true;
      record_idx = i;
      break;
    }
  }
  if(!is_found){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  //..............//
  auto it = clients_.find(machine_id);
  if(it == clients_.end()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  auto target_mac = it->second; 
  auto response = target_mac->call("free_block", block_id);
  if(response.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  auto is_success = response.unwrap()->as<bool>();
  if(!is_success){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  // remove this record from local inode block
  for(uint i = record_idx;i < inode_ptr->get_nblocks();i = i + 2){
    if(i + 3 >= inode_ptr->get_nblocks()){
      inode_ptr->blocks[i] = KInvalidBlockID;
      inode_ptr->blocks[i + 1] = 0;
      break;
    }
    inode_ptr->blocks[i] = inode_ptr->blocks[i + 2];
    inode_ptr->blocks[i + 1] = inode_ptr->blocks[i + 3];
    if(inode_ptr->blocks[i + 2] == KInvalidBlockID){
      break;
    }
  }
  auto write_res = operation_->block_manager_->write_block(inode_block_id, file_inode.data());
  if(write_res.is_err()){
    //! global mutex !//
    global_mtx.unlock();
    //! global mutex !//
    return false;
  }
  //! global mutex !//
  global_mtx.unlock();
  //! global mutex !//
  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  std::vector<std::pair<std::string, inode_id_t>> res_vec(0);
  std::list<DirectoryEntry> list;
  auto read_res = read_directory(operation_.get(), node, list);
  if(read_res.is_err()){
    return res_vec;
  }
  for(const auto &entry : list){
    std::pair<std::string, inode_id_t> vec_element(entry.name, entry.id);
    res_vec.push_back(vec_element);
  }
  return res_vec;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  auto get_res = operation_->get_type_attr(id);
  if(get_res.is_err()){
    return std::tuple<u64, u64, u64, u64, u8>(0, 0, 0, 0, 0);
  }
  auto pair = get_res.unwrap();
  InodeType inode_type = pair.first;
  FileAttr file_attr = pair.second;

  return std::tuple<u64, u64, u64, u64, u8>(file_attr.size, file_attr.atime, file_attr.mtime, file_attr.ctime, static_cast<u8>(inode_type));
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs