#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  //UNIMPLEMENTED();
  std::ostringstream oss;
  //如果src不为空的话需要先加入一个/分隔符
  if(!src.empty()){
    oss << '/';
  }
  oss << filename << ':' << id;
  src.append(oss.str());
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  //UNIMPLEMENTED();
  list.clear();
  std::istringstream iss(src);
  std::string token;
  while(std::getline(iss, token, '/')){
    std::istringstream token_stream(token);
    DirectoryEntry tmp;
    if(std::getline(token_stream, tmp.name, ':') && (token_stream >> tmp.id)){
      list.push_back(tmp);
    }else{
      std::cerr << "format error!" << src << std::endl;
    }
  }
}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  //UNIMPLEMENTED();
  std::list<DirectoryEntry> tmp_list;
  parse_directory(src, tmp_list);
  //...注意迭代器和erase同时使用的时候要注意，erase之后的结果，vector和list是不一样的，list在erase之后每个元素的地址是不发生改变的(由双链表记录前后元素的地址)。而vector的元素的地址是会往前移动的。...//
  std::list<DirectoryEntry>::iterator it = tmp_list.begin();
  while (it != tmp_list.end()){
    if((*it).name == filename){
      //....在erase之前需要先把下一个位置的迭代器记录下来...//
      std::list<DirectoryEntry>::iterator it_tmp = ++it;
      --it;
      tmp_list.erase(it);
      it = it_tmp;
      continue;
    }
    ++it;
  }
  res = dir_list_to_string(tmp_list);
  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  std::vector<u8> dir_vec = (fs->read_file(id)).unwrap();
  std::string dir_string;
  dir_string.assign(dir_vec.begin(), dir_vec.end());
  parse_directory(dir_string, list);
  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  //UNIMPLEMENTED();
  std::string filename_str(name);
  read_directory(this, id, list);
  for (const auto &entry : list) {
    if(entry.name == filename_str){
      return ChfsResult<inode_id_t>(entry.id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  //UNIMPLEMENTED();
  std::list<DirectoryEntry> list;
  if((this->lookup(id, name)).is_ok()){
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }
  std::string filename_str(name);
  inode_id_t allocate_inode_id = (this->alloc_inode(type)).unwrap();
  read_directory(this, id, list);
  DirectoryEntry new_entry;
  new_entry.name = filename_str;
  new_entry.id = allocate_inode_id;
  list.push_back(new_entry);

  std::string new_dir_string = dir_list_to_string(list);
  std::vector<u8> new_dir_vec(new_dir_string.begin(), new_dir_string.end());
  this->write_file(id, new_dir_vec);

  return ChfsResult<inode_id_t>(allocate_inode_id);
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  //UNIMPLEMENTED();
  inode_id_t remove_file_inode_id = (this->lookup(parent, name)).unwrap();
  this->remove_file(remove_file_inode_id);

  std::string name_str(name);
  std::vector<u8> parent_content = (this->read_file(parent)).unwrap();
  std::string parent_content_str(reinterpret_cast<char *>(parent_content.data()), parent_content.size());
  //...注意这个地方rm_from_directory并不是在传入的string自身上作修改，而是返回修改以后的string值...//
  std::string parent_content_str_change = rm_from_directory(parent_content_str, name_str);
  std::vector<u8> new_dir_vec(parent_content_str_change.begin(), parent_content_str_change.end());
  this->write_file(parent, new_dir_vec);
  
  return KNullOk;
}

} // namespace chfs
