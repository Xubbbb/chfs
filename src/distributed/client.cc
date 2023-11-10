#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  auto response = metadata_server_->call("mknode", static_cast<u8>(type), parent, name);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsResult<inode_id_t>(error_code);
  }
  auto inode_id = response.unwrap()->as<inode_id_t>();
  if(inode_id == KInvalidInodeID){
    auto error_code = ErrorType::INVALID;
    return ChfsResult<inode_id_t>(error_code);
  }
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("unlink", parent, name);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsNullResult(error_code);
  }
  auto is_success = response.unwrap()->as<bool>();
  if(!is_success){
    auto error_code = ErrorType::INVALID;
    return ChfsNullResult(error_code);
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  auto response = metadata_server_->call("lookup", parent, name);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsResult<inode_id_t>(error_code);
  }
  auto inode_id = response.unwrap()->as<inode_id_t>();
  if(inode_id == KInvalidInodeID){
    auto error_code = ErrorType::INVALID;
    return ChfsResult<inode_id_t>(error_code);
  }
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("readdir", id);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(error_code);
  }
  auto pair_vec = response.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(pair_vec);
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("get_type_attr", id);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsResult<std::pair<InodeType, FileAttr>>(error_code);
  }
  auto res = response.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  auto type = std::get<4>(res);
  InodeType inode_type;
  if(type == DirectoryType){
    inode_type = InodeType::Directory;
  }
  else if(type == RegularFileType){
    inode_type = InodeType::FILE;
  }
  else{
    inode_type = InodeType::Unknown;
  }
  FileAttr file_attr;
  file_attr.size = std::get<0>(res);
  file_attr.atime = std::get<1>(res);
  file_attr.mtime = std::get<2>(res);
  file_attr.ctime = std::get<3>(res);
  std::pair<InodeType, FileAttr> res_pair(inode_type, file_attr);
  return ChfsResult<std::pair<InodeType, FileAttr>>(res_pair);
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  // first, get the block mapping from metadata server
  // as a client, we don't have any method to know how large a block is in dataserver, so we use default value
  const auto BLOCK_SIZE = DiskBlockSize;
  auto get_block_map_response = metadata_server_->call("get_block_map", id);
  if(get_block_map_response.is_err()){
    auto error_code = get_block_map_response.unwrap_error();
    return ChfsResult<std::vector<u8>>(error_code);
  }
  auto block_info_vec = get_block_map_response.unwrap()->as<std::vector<chfs::BlockInfo>>();
  auto file_sz = block_info_vec.size() * BLOCK_SIZE;
  if(offset + size > file_sz){
    // this error means that you are trying to read data out of range of this file
    auto error_code = ErrorType::INVALID_ARG;
    return ChfsResult<std::vector<u8>>(error_code);
  }
  // now we read data block by block from dataserver to a buffer
  std::vector<u8> res(size);
  //!notice that start block & end block have to be handled specially because they can be not align to block
  auto read_start_idx = offset / BLOCK_SIZE;
  auto read_start_offset = offset % BLOCK_SIZE;
  auto read_end_idx = ((offset + size) % BLOCK_SIZE) ? ((offset + size) / BLOCK_SIZE + 1) : ((offset + size) / BLOCK_SIZE);
  auto read_end_offset = ((offset + size) % BLOCK_SIZE) ? ((offset + size) % BLOCK_SIZE) : BLOCK_SIZE;
  usize current_offset = 0;
  for(auto it = block_info_vec.begin() + read_start_idx;it != block_info_vec.begin() + read_end_idx;++it){
    block_id_t block_id = std::get<0>(*it);
    mac_id_t mac_id = std::get<1>(*it);
    version_t version_id = std::get<2>(*it);
    auto mac_it = data_servers_.find(mac_id);
    if(mac_it == data_servers_.end()){
      auto error_code = ErrorType::INVALID_ARG;
      return ChfsResult<std::vector<u8>>(error_code);
    }
    auto target_mac = mac_it->second;
    
    auto read_response = target_mac->call("read_data", block_id, 0, BLOCK_SIZE, version_id);
    if(read_response.is_err()){
      auto error_code = read_response.unwrap_error();
      return ChfsResult<std::vector<u8>>(error_code);
    }
    auto read_vec = read_response.unwrap()->as<std::vector<u8>>();
    if(it == block_info_vec.begin() + read_start_idx && it == block_info_vec.begin() + (read_end_idx - 1)){
      std::copy(read_vec.begin() + read_start_offset, read_vec.begin() + read_end_offset, res.begin());
      return ChfsResult<std::vector<u8>>(res);
    }
    if(it == block_info_vec.begin() + read_start_idx){
      std::copy(read_vec.begin() + read_start_offset, read_vec.end(), res.begin());
      current_offset += BLOCK_SIZE - read_start_offset;
    }
    else if(it == block_info_vec.begin() + (read_end_idx - 1)){
      std::copy_n(read_vec.begin(), read_end_offset, res.begin() + current_offset);
      current_offset += read_end_offset;
    }
    else{
      std::copy_n(read_vec.begin(), BLOCK_SIZE, res.begin() + current_offset);
      current_offset += BLOCK_SIZE;
    }
  }
  return ChfsResult<std::vector<u8>>(res);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();

  const auto BLOCK_SIZE = DiskBlockSize;
  auto write_length = data.size();
  auto get_block_map_response = metadata_server_->call("get_block_map", id);
  if(get_block_map_response.is_err()){
    auto error_code = get_block_map_response.unwrap_error();
    return ChfsNullResult(error_code);
  }
  auto block_info_vec = get_block_map_response.unwrap()->as<std::vector<chfs::BlockInfo>>();
  auto old_file_sz = block_info_vec.size() * BLOCK_SIZE;

  if(offset + write_length > old_file_sz){
    //...if we need to alloc enough block first, then to write...//
    auto new_block_num = ((offset + write_length) % BLOCK_SIZE) ? ((offset + write_length) / BLOCK_SIZE + 1) : ((offset + write_length) % BLOCK_SIZE);
    auto old_block_num = block_info_vec.size();
    // alloc some new block
    for(auto i = old_block_num;i < new_block_num;++i){
      auto alloc_response = metadata_server_->call("alloc_block", id);
      if(alloc_response.is_err()){
        auto error_code = alloc_response.unwrap_error();
        return ChfsNullResult(error_code);
      }
      //? we have two choices: 1. alloc and get a new block_info then put it into block_info_vec 2. alloc and get a new block_info_vec from metadata server.
      //? here we choose the first choice to implement, otherwise there is meaningless for function allocate_block to return a BlockInfo
      auto new_block_info = alloc_response.unwrap()->as<BlockInfo>();
      block_info_vec.push_back(new_block_info);
    }
    //...if we need to alloc enough block first, then to write...//
  }

  //...if we don't need to alloc more block or we have allocated enough block to support our write operation...//
  auto write_start_idx = offset / BLOCK_SIZE;
  auto write_start_offset = offset % BLOCK_SIZE;
  auto write_end_idx = ((offset + write_length) % BLOCK_SIZE) ? ((offset + write_length) / BLOCK_SIZE + 1) : ((offset + write_length) / BLOCK_SIZE);
  auto write_end_offset = ((offset + write_length) % BLOCK_SIZE) ? ((offset + write_length) % BLOCK_SIZE) : BLOCK_SIZE;
  usize current_offset = 0;
  for(auto it = block_info_vec.begin() + write_start_idx;it != block_info_vec.begin() + write_end_idx;++it){
    block_id_t block_id = std::get<0>(*it);
    mac_id_t mac_id = std::get<1>(*it);
    auto mac_it = data_servers_.find(mac_id);
    if(mac_it == data_servers_.end()){
      auto error_code = ErrorType::INVALID_ARG;
      return ChfsNullResult(error_code);
    }
    auto target_mac = mac_it->second;
    std::vector<u8> write_buf;
    usize per_write_offset = 0;
    if(it == block_info_vec.begin() + write_start_idx && it == block_info_vec.begin() + (write_end_idx - 1)){
      auto write_response = target_mac->call("write_data", block_id, write_start_offset, data);
      if(write_response.is_err()){
        auto error_code = write_response.unwrap_error();
        return ChfsNullResult(error_code);
      }
      auto is_success = write_response.unwrap()->as<bool>();
      if(!is_success){
        auto error_code = ErrorType::INVALID;
        return ChfsNullResult(error_code);
      }
      return KNullOk;
    }
    if(it == block_info_vec.begin() + write_start_idx){
      write_buf.resize(BLOCK_SIZE - write_start_offset);
      std::copy_n(data.begin(), BLOCK_SIZE - write_start_offset, write_buf.begin());
      per_write_offset = write_start_offset;
      current_offset += BLOCK_SIZE - write_start_offset;
    }
    else if(it == block_info_vec.begin() + (write_end_idx - 1)){
      write_buf.resize(write_end_offset);
      std::copy_n(data.begin() + current_offset, write_end_offset, write_buf.begin());
      per_write_offset = 0;
      current_offset += write_end_offset;
    }
    else{
      write_buf.resize(BLOCK_SIZE);
      std::copy_n(data.begin() + current_offset, BLOCK_SIZE, write_buf.begin());
      per_write_offset = 0;
      current_offset += BLOCK_SIZE;
    }
    auto write_response = target_mac->call("write_data", block_id, per_write_offset, write_buf);
    if(write_response.is_err()){
      auto error_code = write_response.unwrap_error();
      return ChfsNullResult(error_code);
    }
    auto is_success = write_response.unwrap()->as<bool>();
    if(!is_success){
      auto error_code = ErrorType::INVALID;
      return ChfsNullResult(error_code);
    }
  }
  return KNullOk;
  //...if we don't need to alloc more block...//
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
  auto response = metadata_server_->call("free_block", id, block_id, mac_id);
  if(response.is_err()){
    auto error_code = response.unwrap_error();
    return ChfsNullResult(error_code);
  }
  auto is_success = response.unwrap()->as<bool>();
  if(!is_success){
    auto error_code = ErrorType::NotExist;
    return ChfsNullResult(error_code);
  }
  return KNullOk;
}

} // namespace chfs