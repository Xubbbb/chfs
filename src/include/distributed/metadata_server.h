//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// dataserver.h
//
// Identification: src/include/distributed/dataserver.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/config.h"
#include "common/util.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "metadata/manager.h"
#include "filesystem/operations.h"
#include "distributed/commit_log.h"

namespace chfs {

/**
 * `MetadataServer` is the master server in chfs where the metadata is stored.
 *
 * It controls the inode and other metadata of a file, basically the position of
 * each block in the file. It is responsible for serving metadata operations
 * like file creation, deletion and query position. It also holds the
 * information about the directory structure with the original `InodeManager`
 * structure.
 *
 * In LAB2, we assume that there's only one metadata server and many data
 * servers, just like GFS. And the data block's position is stored in the block
 * since it should be persisted if the server is crashed.
 *
 * The process of writing an empty file(which means at least one block data
 * should be allocated) is as follows:
 * 1. Client asks the metadata server for a block mapping, so it can know the
 * position of blocks in the file.
 * 2. Metadata server reads the content in the disk and return an empty map
 * since there's no block currently.
 * 3. Client gets an empty map and realize that it needs to allocate a block, so
 * it sends an `alloc_block` rpc to the metadata server.
 * 4. Metadata server allocates a block and returns the id & position to the
 * client.
 * 5. Client writes the data to the data server with the given id.
 *
 * It's noticeable that whether a file needs a new block is determined by the
 * client. For example, if currently the file has one block(which means the file
 * size is less than 2MB). A client wants to write bytes at offset like 2MB + 1,
 * and it realize that it needs to allocate one more block. And then it sends a
 * `alloc_block` rpc to metadata server.
 */
const u8 RegularFileType = 1;
const u8 DirectoryType = 2;

using BlockInfo = std::tuple<block_id_t, mac_id_t, version_t>;

class MetadataServer {
  const size_t num_worker_threads = 4; // worker threads for rpc handlers
public:
  /**
   * Start a metadata server that listens on `localhost` with the given port.
   *
   * It receives requests from client and sometimes send requests to
   * data server for block allocating or deleting.
   *
   * @param port: The port number to listen on.
   * @param data_path: The file path where persists data.
   * @param is_log_enabled: Whether to enable the commit log.
   * @param is_checkpoint_enabled: Whether to enable the checkpoint.
   * @param may_failed: Whether the metadata server persist data may fail.
   */
  MetadataServer(u16 port, const std::string &data_path = "/tmp/inode_data",
                 bool is_log_enabled = false,
                 bool is_checkpoint_enabled = false, bool may_failed = false);

  /**
   * Start a metadata server listens on `address:port`.
   *
   * @param address: The address to be bound.
   * @param port: The port number to listen on.
   * @param data_path: The file persists the data.
   * @param is_log_enabled: Whether to enable the commit log.
   * @param is_checkpoint_enabled: Whether to enable the checkpoint.
   * @param may_failed: Whether the metadata server persist data may fail.
   */
  MetadataServer(std::string const &address, u16 port,
                 const std::string &data_path = "/tmp/inode_data",
                 bool is_log_enabled = false,
                 bool is_checkpoint_enabled = false, bool may_failed = false);

  /**
   * A RPC handler for client. It create a regular file or directory on metadata
   * server.
   *
   * @param type: Create a dir or regular file
   * @param parent: The parent directory of the node to be created.
   * @param name: The name of the node to be created.
   *
   * Using `const std::string&` rather than `const char *`. see
   * https://github.com/rpclib/rpclib/issues/280.
   *
   * Also note that we don't use an enum for `type` since it needs extra
   * support by `msgpack` for serialization.
   */
  auto mknode(u8 type, inode_id_t parent, const std::string &name)
      -> inode_id_t;

  /**
   * A RPC handler for client. It deletes an file on metadata server from its
   * parent.
   *
   * @param parent: The inode id of the parent directory.
   * @param name: The name of the file to be deleted.
   */
  auto unlink(inode_id_t parent, const std::string &name) -> bool;

  /**
   * A RPC handler for client. It looks up the dir and return the inode id of
   * the given name.
   *
   * @param parent: The parent directory of the node to be found.
   * @param name: The name of the node to be removed.
   */
  auto lookup(inode_id_t parent, const std::string &name) -> inode_id_t;

  /**
   * A RPC handler for client. It returns client a list about the position of
   * each block in a file.
   *
   * @param id: The inode id of the file.
   */
  auto get_block_map(inode_id_t id) -> std::vector<BlockInfo>;

  // [transaction] //
  auto get_block_map_from_memory(inode_id_t id, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> std::vector<BlockInfo>;
  // [transaction] //


  /**
   * A RPC handler for client. It allocate a new block for a file on data server
   * and return the logic block id & node id to client.
   *
   * @param id: The inode id of the file.
   */
  auto allocate_block(inode_id_t id) -> BlockInfo;

  /**
   * A RPC handler for client. It removes a block from a file on data server
   * and delete its record on metadata server.
   *
   * @param id: The inode id of the file
   * @param block: The block id of the file
   * @param machine_id: The machine id of the block
   */
  auto free_block(inode_id_t id, block_id_t block, mac_id_t machine_id) -> bool;

  /**
   * A RPC handler for client. It returns the content of a directory.
   *
   * @param node: The inode id of the directory
   */
  auto readdir(inode_id_t node)
      -> std::vector<std::pair<std::string, inode_id_t>>;

  /**
   * A RPC handler for client. It returns the type and attribute of a file
   *
   * @param id: The inode id of the file
   * 
   * @return: a tuple of <size, atime, mtime, ctime, type>
   */
  auto get_type_attr(inode_id_t id) -> std::tuple<u64, u64, u64, u64, u8>;

  /**
   * Register a data server to the metadata server. It'll create a RPC
   * connection between the data server and metadata server. It should be called
   * before the metadata server starts to run. Once it's started, it wouldn't
   * allow any data server to register.
   *
   * @param address: The address of the data server.
   * @param port: The port of the data server,
   * @param reliable: Whether the network is reliable or not.
   */
  auto reg_server(const std::string &address, u16 port, bool reliable) -> bool;

  /**
   * Start the metadata server, which means it's ready to receive requests from
   * client.
   *
   * @return: False if the server has already been launched.
   */
  auto run() -> bool;

  /**
   * Recover the system from log
   */
  auto recover() -> void {
    if (!is_log_enabled_) {
      std::cerr << "Log not enabled\n";
      return;
    }
    operation_->block_manager_->set_may_fail(false);
    commit_log->recover();
    operation_->block_manager_->set_may_fail(true);
  }

  /**
   * Get log entries
   */
  auto get_log_entries() -> usize {
    if (is_log_enabled_) {
      return commit_log->get_log_entry_num();
    } else {
      std::cerr << "Log not enabled\n";
      return 0;
    }
  }

private:
  /**
   * Helper function for binding rpc handlers
   */
  inline auto bind_handlers();

  /**
   * Helper function for initializing the fs.
   *
   * @param data_path: The file path where persists data.
   */
  inline auto init_fs(const std::string &data_path);

  std::unique_ptr<RpcServer> server_; // Receiving requests from the client
  std::shared_ptr<FileOperation> operation_; // Real metadata handler
  std::map<mac_id_t, std::shared_ptr<RpcClient>>
      clients_;              // Sending requests to data server
  mac_id_t num_data_servers; // The number of data servers
  bool running;
  // Control which data server node allocates the new block
  RandomNumberGenerator generator;

  // Log related
  [[maybe_unused]] std::shared_ptr<chfs::CommitLog> commit_log;
  bool is_log_enabled_;
  bool may_failed_;
  [[maybe_unused]] bool is_checkpoint_enabled_;

  /**
   * {You can add anything you want here}
   */
  std::mutex global_mtx;
};

} // namespace chfs