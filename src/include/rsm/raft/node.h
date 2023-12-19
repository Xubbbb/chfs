#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    int voted_for;
    int received_votes;
    int commit_index;
    int last_applied;
    int snapshot_last_index;
    int snapshot_last_term;

    unsigned long last_receive_timestamp;
    std::unique_ptr<int[]> next_index;
    std::unique_ptr<int[]> match_index;
    // Just a list in memory, volatile
    std::vector<LogEntry<Command>> log_entry_list;
    //snapshot
    std::vector<u8> last_snapshot;
    
    int logic2physical(int logic_index){
        return logic_index - snapshot_last_index;
    }
    
    int physical2logic(int physical_index){
        return physical_index + snapshot_last_index;
    }

};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1),
    voted_for(-1),
    received_votes(0),
    commit_index(0),
    last_applied(0),
    snapshot_last_index(0),
    snapshot_last_term(0)
{
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

   /* Lab3: Your code here */
   state = std::make_unique<StateMachine>();
   thread_pool = std::make_unique<ThreadPool>(32);
   last_receive_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
   next_index.reset(new int[configs.size()]);
   match_index.reset(new int[configs.size()]);
   /**
    * Notice that the first log index is 1 instead of 0. 
    * To simplify the programming, you can append an empty log entry to the logs at the very beginning. 
    * And since the 'lastApplied' index starts from 0, 
    * the first empty log entry will never be applied to the state machine.
   */
    LogEntry<Command> init_entry;
    init_entry.term = 0;
    init_entry.index = 0;
    log_entry_list.push_back(init_entry);
    last_snapshot = state->snapshot();

    // RaftLog for persistence
    std::string node_log_filename = "/tmp/raft_log/node" + std::to_string(node_id);
    bool is_recover = is_file_exist(node_log_filename);
    auto block_manager = std::shared_ptr<BlockManager>(new BlockManager(node_log_filename));
    log_storage = std::make_unique<RaftLog<Command>>(block_manager, is_recover);
    if(is_recover){
        log_storage->recover(current_term, voted_for, log_entry_list, last_snapshot, snapshot_last_index, snapshot_last_term);
        state->apply_snapshot(last_snapshot);
        last_applied = snapshot_last_index;
        commit_index = snapshot_last_index;
    }
    else{
        log_storage->updateMetaData(current_term, voted_for);
        log_storage->updateLogs(log_entry_list);
        log_storage->updateSnapshot(last_snapshot);
    }

    RAFT_LOG("A new raft node init");
    rpc_server->run(true, configs.size()); 
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();
    
    next_index.reset();
    match_index.reset();
    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here */

    RAFT_LOG("RaftNode start");
    //! ATTENTION: rpc_clients_map init must be implemented at start(), can't implement it in construct function!!!
    /** Why?
     * Because the construction of RpcClient will connect to corresponding RpcServer.
     * But RpcServer's construction is in RaftNode's construct function.
     * If we implement RpcClient's construction in RaftNode's construct function,
     * when a RaftNode is constructing we can't promise other RaftNode's RpcServer has been established!
     * So we have to wait until every RaftNode's RpcServer has been established then construct RpcClient.
    */
    for(auto config_it = node_configs.begin();config_it != node_configs.end();++config_it){
        rpc_clients_map.insert(std::make_pair(config_it->node_id, std::make_unique<RpcClient>(config_it->ip_address, config_it->port, true)));
    }
    stopped.store(false);
    // Wait until all threads start then begin to work.
    //std::unique_lock<std::mutex> lock(mtx);
    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);
    RAFT_LOG("RaftNode start done");

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    stopped.store(true);
    background_election->join();
    background_ping->join();
    background_commit->join();
    background_apply->join();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    /* Lab3: Your code here */
    if(role == RaftRole::Leader){
        return std::make_tuple(true, current_term);
    }
    return std::make_tuple(false, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    if( role != RaftRole::Leader){
        return std::make_tuple(false, current_term, log_entry_list.size());
    }
    else{
        LogEntry<Command> logEntry;
        logEntry.index = physical2logic(log_entry_list.size());
        logEntry.term = current_term;
        Command command;
        int command_size = command.size();
        command.deserialize(cmd_data, command_size);
        logEntry.command = command;
        log_entry_list.push_back(logEntry);
        //[ persistency ]//
        log_storage->updateLogs(log_entry_list);
        //[ persistency ]//
        return std::make_tuple(true, current_term, logEntry.index);
    }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */ 
    std::unique_lock<std::mutex> lock(mtx);
    int start_index = logic2physical(last_applied);
    snapshot_last_term = log_entry_list[logic2physical(last_applied)].term;
    snapshot_last_index = last_applied;
    last_snapshot = state->snapshot();

    std::vector<LogEntry<Command>> new_log_entry_list;
    for(int i = start_index;i < log_entry_list.size();++i){
        new_log_entry_list.push_back(log_entry_list[i]);
    }
    log_entry_list = new_log_entry_list;

    log_storage->updateSnapshot(last_snapshot);
    log_storage->updateLogs(log_entry_list);
    //!debug//
    RAFT_LOG("Save snapshot success, snapshot_last_index: %d, snapshot_last_term: %d, log_entry_list size: %lu", snapshot_last_index, snapshot_last_term, log_entry_list.size());
    //!debug//

    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return state->snapshot();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    /* Lab3: Your code here */
    // unique_lock will unlock the mtx automatically when it's been deleted, so we don't need to unlock it manually.
    std::unique_lock<std::mutex> lock(mtx);
    //!debug//
    //RAFT_LOG("Receive vote request from %d", args.candidateId);
    //!debug//
    if(args.term < current_term){
        //!debug//
        //RAFT_LOG("refuse reason 1");
        //!debug//
        RequestVoteReply reply;
        reply.voteFollowerId = my_id;
        reply.term = current_term;
        reply.voteGranted = false;
        return reply;
    }
    if(args.term == current_term && voted_for != -1 && voted_for != args.candidateId){
        //!debug//
        //RAFT_LOG("refuse reason 2, args.term: %d, currentterm: %d, voted for: %d", args.term, current_term, voted_for);
        //!debug//
        RequestVoteReply reply;
        reply.voteFollowerId = my_id;
        reply.term = current_term;
        reply.voteGranted = false;
        return reply;
    }
    //!debug//
    //RAFT_LOG("Is deciding whether to vote, arg_last_term: %d, arg_last_index: %d, my last term: %d, my last index: %lu", args.lastLogTerm, args.lastLogIndex, log_entry_list[log_entry_list.size() - 1].term, log_entry_list.size() - 1);
    //!debug//
    role = RaftRole::Follower;
    current_term = args.term;
    voted_for = -1;
    if(log_entry_list[log_entry_list.size() - 1].term > args.lastLogTerm
        || (log_entry_list[log_entry_list.size() - 1].term == args.lastLogTerm && physical2logic(log_entry_list.size() - 1) > args.lastLogIndex)){
        /**
         * This means that candidate’s log is NOT at
         * least as up-to-date as receiver’s log
        */
        //!debug//
        RAFT_LOG("Refuse to vote to %d, because I am newer! args.lastLogTerm: %d, args.lastLogIndex: %d, my last term: %d, my last index: %d", args.candidateId, args.lastLogTerm, args.lastLogIndex, log_entry_list[log_entry_list.size() - 1].term, physical2logic(log_entry_list.size() - 1));
        //!debug//
        RequestVoteReply reply;
        reply.voteFollowerId = my_id;
        reply.term = current_term;
        reply.voteGranted = false;

        //[ persistency ]//
        log_storage->updateMetaData(current_term, voted_for);
        //[ persistency ]//
        return reply;
    }
    voted_for = args.candidateId;
    RequestVoteReply reply;
    reply.voteFollowerId = my_id;
    reply.term = current_term;
    reply.voteGranted = true;
    last_receive_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
    leader_id = args.candidateId;

    //[ persistency ]//
    log_storage->updateMetaData(current_term, voted_for);
    //[ persistency ]//
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    //!debug//
    //RAFT_LOG("Receive handle vote from %d", target);
    //!debug//
    last_receive_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
    const auto MACHINE_NUM = rpc_clients_map.size();
    if(reply.term > current_term){
        role = RaftRole::Follower;
        current_term = reply.term;
        voted_for = -1;
        //[ persistency ]//
        log_storage->updateMetaData(current_term, voted_for);
        //[ persistency ]//
        return;
    }
    if(role != RaftRole::Candidate || arg.term < current_term){
        // This vote is out of validity
        return;
    }
    if(reply.voteGranted){
        ++received_votes;
        if(received_votes >= MACHINE_NUM / 2 + 1){
            // This candidate receive majority votes
            RAFT_LOG("I am new leader now");
            role = RaftRole::Leader;
            /**
             * next_index:initialized to leader last log index + 1
             * match_index:initialized to 0, increases monotonically
            */
            for(int i = 0;i < node_configs.size();++i){
                next_index[i] = physical2logic(log_entry_list.size());
                match_index[i] = 0;
            }
        }
    }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    bool is_meta_changed = false;
    bool is_log_changed = false;
    AppendEntriesReply reply;
    AppendEntriesArgs<Command> arg = transform_rpc_append_entries_args<Command>(rpc_arg);
    if(leader_id == arg.leaderId){
        last_receive_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
    }
    //!debug//
    //RAFT_LOG("Receive Real AppendEntriesArgs from %d", rpc_arg.leaderId);
    //!debug//
    if(arg.term > current_term){
        if(leader_id != arg.leaderId){
            leader_id = arg.leaderId;
            last_receive_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
        }
        role = RaftRole::Follower;
        current_term = arg.term;
        voted_for = -1;
        is_meta_changed = true;
        /**
         * Reply false if log doesn’t contain an entry at prevLogIndex
         * whose term matches prevLogTerm
        */
        /**
         * After we introduce the snapshot, we will face a problem: 
         * If arg.prevLogIndex is smaller than snapshot_last_index,
         * how can we access log_entry_list[arg.prevLogIndex].term?
         * If arg.prevLogIndex smaller than snapshot_last_index,
         * it means that the log_entry_list[arg.prevLogIndex] is committed and applied to the state machine.
         * so it must be the same as arg.prevLogTerm. If arg.prevLogIndex is bigger than snapshot_last_index,
         * than we can access log_entry_list[arg.prevLogIndex].term
        */
        if(physical2logic(log_entry_list.size() - 1) < arg.prevLogIndex ||
            (logic2physical(arg.prevLogIndex) >= 0 && log_entry_list[logic2physical(arg.prevLogIndex)].term != arg.prevLogTerm)){
            //!debug//
            //RAFT_LOG("Reply false! my last index:%lu, my last term:%d, prevLogIndex:%d, prevLogTerm:%d", log_entry_list.size() - 1, log_entry_list[log_entry_list.size() - 1].term, arg.prevLogIndex, arg.prevLogTerm);
            //!debug//
            reply.term = current_term;
            reply.success = false;
        }
        else{
            /**
             * Now we promise that the entry before(including) prevLog is the same between 
             * Follower and Leader. So we just need to append new entries from (prevIndex + 1)
             * If Follower has inconsitent entry, discard them
            */

            // log_entry_list.resize(arg.prevLogIndex + 1);
            // std::vector<LogEntry<Command>> new_entry_list = arg.logEntryList;
            // for(const LogEntry<Command> &entry : new_entry_list){
            //     log_entry_list.push_back(entry);
            // }
            // is_log_changed = true;
            // /**
            //  * If leaderCommit > commitIndex, set commitIndex =
            //  * min(leaderCommit, index of last new entry)
            // */
            // int last_new_entry_index = log_entry_list.size() - 1;
            // if(arg.leaderCommit > commit_index){
            //     commit_index = arg.leaderCommit < last_new_entry_index ? arg.leaderCommit : last_new_entry_index;
            // }
            // reply.term = current_term;
            // reply.success = true;

            if(arg.logEntryList.empty()){
                // This is a heartbeat
                // int last_new_entry_index = physical2logic(log_entry_list.size() - 1);
                int last_new_entry_index = arg.prevLogIndex;
                if(arg.leaderCommit > commit_index){
                    commit_index = arg.leaderCommit < last_new_entry_index ? arg.leaderCommit : last_new_entry_index;
                }
                reply.term = current_term;
                reply.success = true;
            }
            else{ // This is a real append entries
                // Obey the rule, we first check which entry is
                // the first entry different from leader's, and then erase all entries behind it.
                // then append leader's entries
                std::vector<LogEntry<Command>> new_entry_list = arg.logEntryList;
                if(new_entry_list[0].index > snapshot_last_index){
                    auto new_it = new_entry_list.begin();
                    auto log_it = log_entry_list.begin() + logic2physical(new_entry_list[0].index);
                    for(;;++new_it, ++log_it){
                        if(log_it == log_entry_list.end()){
                            for(;new_it != new_entry_list.end();++new_it){
                                log_entry_list.push_back(*new_it);
                                is_log_changed = true;
                            }
                            break;
                        }
                        if(new_it == new_entry_list.end()){
                            // This means that all entries in new_entry_list are the same as log_entry_list
                            break;
                        }
                        if(log_it->term != new_it->term){
                            // This means that the first entry different from leader's is found
                            log_entry_list.erase(log_it, log_entry_list.end());
                            is_log_changed = true;
                            for(;new_it != new_entry_list.end();++new_it){
                                log_entry_list.push_back(*new_it);
                            }
                            break;
                        }
                    }
                }
                else{
                    // This means a part of new_entry_list is in snapshot,
                    // so we need to erase the part of new_entry_list which is in snapshot
                    // We can promise that the entry in snapshot is the same as leader's
                    
                    // 1. If all new_entry_list is in snapshot, we don't need to do anything
                    if(new_entry_list[new_entry_list.size() - 1].index <= snapshot_last_index){
                        // This means that all entries in new_entry_list are in snapshot
                    }
                    // 2. If a part of new_entry_list in snapshot
                    else{
                        auto log_it = log_entry_list.begin() + 1;
                        auto new_it = new_entry_list.begin() + (snapshot_last_index + 1 - new_entry_list[0].index);
                        for(;;++new_it, ++log_it){
                            if(log_it == log_entry_list.end()){
                                for(;new_it != new_entry_list.end();++new_it){
                                    log_entry_list.push_back(*new_it);
                                    is_log_changed = true;
                                }
                                break;
                            }
                            if(new_it == new_entry_list.end()){
                                // This means that all entries in new_entry_list are the same as log_entry_list
                                break;
                            }
                            if(log_it->term != new_it->term){
                                // This means that the first entry different from leader's is found
                                log_entry_list.erase(log_it, log_entry_list.end());
                                is_log_changed = true;
                                for(;new_it != new_entry_list.end();++new_it){
                                    log_entry_list.push_back(*new_it);
                                }
                                break;
                            }
                        }
                    }
                }
                int last_new_entry_index = physical2logic(log_entry_list.size() - 1);
                if(arg.leaderCommit > commit_index){
                    commit_index = arg.leaderCommit < last_new_entry_index ? arg.leaderCommit : last_new_entry_index;
                }
                reply.term = current_term;
                reply.success = true;
            }
        }
    }
    else if(arg.term == current_term){
        if(leader_id != arg.leaderId){
            leader_id = arg.leaderId;
            last_receive_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
        }
        role = RaftRole::Follower;
        if(physical2logic(log_entry_list.size() - 1) < arg.prevLogIndex ||
            (logic2physical(arg.prevLogIndex) >= 0 && log_entry_list[logic2physical(arg.prevLogIndex)].term != arg.prevLogTerm)){
            //!debug//
            //RAFT_LOG("Reply false! my last index:%lu, my last term:%d, prevLogIndex:%d, prevLogTerm:%d", log_entry_list.size() - 1, log_entry_list[log_entry_list.size() - 1].term, arg.prevLogIndex, arg.prevLogTerm);
            //!debug//
            reply.term = current_term;
            reply.success = false;
        }
        else{
            if(arg.logEntryList.empty()){
                // This is a heartbeat
                // int last_new_entry_index = physical2logic(log_entry_list.size() - 1);
                int last_new_entry_index = arg.prevLogIndex;
                if(arg.leaderCommit > commit_index){
                    commit_index = arg.leaderCommit < last_new_entry_index ? arg.leaderCommit : last_new_entry_index;
                }
                reply.term = current_term;
                reply.success = true;
            }
            else{ // This is a real append entries
                // Obey the rule, we first check which entry is
                // the first entry different from leader's, and then erase all entries behind it.
                // then append leader's entries
                std::vector<LogEntry<Command>> new_entry_list = arg.logEntryList;
                if(new_entry_list[0].index > snapshot_last_index){
                    auto new_it = new_entry_list.begin();
                    auto log_it = log_entry_list.begin() + logic2physical(new_entry_list[0].index);
                    for(;;++new_it, ++log_it){
                        if(log_it == log_entry_list.end()){
                            for(;new_it != new_entry_list.end();++new_it){
                                log_entry_list.push_back(*new_it);
                                is_log_changed = true;
                            }
                            break;
                        }
                        if(new_it == new_entry_list.end()){
                            // This means that all entries in new_entry_list are the same as log_entry_list
                            break;
                        }
                        if(log_it->term != new_it->term){
                            // This means that the first entry different from leader's is found
                            log_entry_list.erase(log_it, log_entry_list.end());
                            is_log_changed = true;
                            for(;new_it != new_entry_list.end();++new_it){
                                log_entry_list.push_back(*new_it);
                            }
                            break;
                        }
                    }
                }
                else{
                    // This means a part of new_entry_list is in snapshot,
                    // so we need to erase the part of new_entry_list which is in snapshot
                    // We can promise that the entry in snapshot is the same as leader's
                    
                    // 1. If all new_entry_list is in snapshot, we don't need to do anything
                    if(new_entry_list[new_entry_list.size() - 1].index <= snapshot_last_index){
                        // This means that all entries in new_entry_list are in snapshot
                    }
                    // 2. If a part of new_entry_list in snapshot
                    else{
                        auto log_it = log_entry_list.begin() + 1;
                        auto new_it = new_entry_list.begin() + (snapshot_last_index + 1 - new_entry_list[0].index);
                        for(;;++new_it, ++log_it){
                            if(log_it == log_entry_list.end()){
                                for(;new_it != new_entry_list.end();++new_it){
                                    log_entry_list.push_back(*new_it);
                                    is_log_changed = true;
                                }
                                break;
                            }
                            if(new_it == new_entry_list.end()){
                                // This means that all entries in new_entry_list are the same as log_entry_list
                                break;
                            }
                            if(log_it->term != new_it->term){
                                // This means that the first entry different from leader's is found
                                log_entry_list.erase(log_it, log_entry_list.end());
                                is_log_changed = true;
                                for(;new_it != new_entry_list.end();++new_it){
                                    log_entry_list.push_back(*new_it);
                                }
                                break;
                            }
                        }
                    }
                }
                int last_new_entry_index = physical2logic(log_entry_list.size() - 1);
                if(arg.leaderCommit > commit_index){
                    commit_index = arg.leaderCommit < last_new_entry_index ? arg.leaderCommit : last_new_entry_index;
                }
                reply.term = current_term;
                reply.success = true;
            }
        }
    }
    else{
        // This means this leader is out of date
        reply.term = current_term;
        reply.success = false;
    }
    //[ persistency ]//
    if(is_meta_changed){
        log_storage->updateMetaData(current_term, voted_for);
    }
    if(is_log_changed){
        log_storage->updateLogs(log_entry_list);
    }
    //[ persistency ]//
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    last_receive_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
    /** This means this term has been finished.
     *  A new candidate has been trying to become a new leader.
     *  Or even has been become a new leader.
     *  Now it should give up leader and become follower again.
     *  And then it don't have ability to do anything about this reply.
    */ 
    if(reply.term > current_term){
        role = RaftRole::Follower;
        current_term = reply.term;
        voted_for = -1;
        //!debug//
        RAFT_LOG("I am follower now because %d is newer than me!", node_id);
        //!debug//
        //[ persistency ]//
        log_storage->updateMetaData(current_term, voted_for);
        //[ persistency ]//
        return;
    }
    if( role != RaftRole::Leader){
        // If this server has been not a leader anymore, it don't have ability to do anything about this reply.
        return;
    }
    // if(reply.isHeartbeat){
    //     return;
    // }
    if(reply.success == false){
        /**
         * There are two situation will cause reply's false
         * 1. arg.term < reply.term
         * 2. prevLogIndex's entry is inconsistent which means next_index of this follower should be smaller
        */
        if(reply.term > arg.term){
            /**
             * We can't promise that arg.term is the same as current_term so we need to handle it specifically here
            */
            return;
        }
        else{
            
            //!debug//
            //RAFT_LOG("%d's next_index - 1 : %d", node_id, next_index[node_id] - 1);
            //RAFT_LOG("%d, %d, %d", arg.term, arg.prevLogTerm, arg.prevLogIndex);
            //!debug//
            next_index[node_id] = next_index[node_id] - 1;
        }
    }
    else{
        //! Attention: we must choose bigger one, arg.prevLogIndex + arg.logEntryList.size() can be smaller than now match_index[node_id]
        //! Because a former reply can be received later!!!
        int reply_match_index = arg.prevLogIndex + arg.logEntryList.size();
        if(reply_match_index > match_index[node_id]){
            match_index[node_id] = reply_match_index;
        }
        //!debug//
        //RAFT_LOG("%d's next_index old:%d, new:%d", node_id, next_index[node_id], match_index[node_id] + 1);
        //!debug//
        next_index[node_id] = match_index[node_id] + 1;
        /**
         * If there exists an N such that N > commitIndex, a majority
         * of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N 
        */
        const auto MACHINE_NUM = rpc_clients_map.size();
        const auto LAST_INDEX = log_entry_list[log_entry_list.size() - 1].index;
        for(int N = LAST_INDEX;N > commit_index;--N){
            if(log_entry_list[logic2physical(N)].term != current_term){
                break;
            }
            // This node itself must match
            int num_of_matched = 1;
            for(auto map_it = rpc_clients_map.begin();map_it != rpc_clients_map.end();++map_it){
                if(map_it->first == my_id){
                    continue;
                }
                if(match_index[map_it->first] >= N){
                    ++num_of_matched;
                }
                if(num_of_matched >= MACHINE_NUM / 2 + 1){
                    commit_index = N;
                    break;
                }
            }
            if(commit_index == N){
                break;
            }
        }
    }
    return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{   
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    bool is_meta_changed = false;
    if(args.term < current_term){
        InstallSnapshotReply reply;
        reply.term = current_term;
        return reply;
    }
    role = RaftRole::Follower;
    if(args.term > current_term){
        voted_for = -1;
        current_term = args.term;
        is_meta_changed = true;
    }
    leader_id = args.leaderId;
    last_receive_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
    // For convenience, we will send all snapshot data in one RPC
    if(snapshot_last_index >= args.lastIncludedIndex){
        // This means this snapshot is out of date, don't need to apply it
        InstallSnapshotReply reply;
        reply.term = current_term;
        if(is_meta_changed){
            //[ persistency ]//
            log_storage->updateMetaData(current_term, voted_for);
            //[ persistency ]//
        }
        return reply;
    }
    //If existing log entry has same index and term as snapshot’s
    //last included entry, retain log entries following it and reply
    if(logic2physical(args.lastIncludedIndex) < log_entry_list.size()
        && log_entry_list[logic2physical(args.lastIncludedIndex)].term == args.lastIncludedTerm){
        // remove in snapshot log
        log_entry_list.erase(log_entry_list.begin() + 1, log_entry_list.begin() + (logic2physical(args.lastIncludedIndex) + 1));
        log_entry_list[0].term = args.lastIncludedTerm;
        log_entry_list[0].index = args.lastIncludedIndex;
        snapshot_last_index = args.lastIncludedIndex;
        snapshot_last_term = args.lastIncludedTerm;
        state->apply_snapshot(args.data);
        last_snapshot = args.data;
        last_applied = args.lastIncludedIndex;
        commit_index = (commit_index < args.lastIncludedIndex) ? args.lastIncludedIndex : commit_index;
    }
    // Else discard the entire log
    else{
        log_entry_list.clear();
        LogEntry<Command> init_entry;
        init_entry.term = args.lastIncludedTerm;
        init_entry.index = args.lastIncludedIndex;
        log_entry_list.push_back(init_entry);
        snapshot_last_index = args.lastIncludedIndex;
        snapshot_last_term = args.lastIncludedTerm;
        state->apply_snapshot(args.data);
        last_snapshot = args.data;
        last_applied = args.lastIncludedIndex;
        commit_index = args.lastIncludedIndex;
    }

    //[ persistency ]//
    if(is_meta_changed){
        log_storage->updateMetaData(current_term, voted_for);
    }
    log_storage->updateLogs(log_entry_list);
    log_storage->updateSnapshot(last_snapshot);
    //[ persistency ]//
    InstallSnapshotReply reply;
    reply.term = current_term;
    return reply;
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    last_receive_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
    if(reply.term > current_term){
        role = RaftRole::Follower;
        current_term = reply.term;
        voted_for = -1;
        //!debug//
        RAFT_LOG("I am follower now because %d is newer than me!", node_id);
        //!debug//
        //[ persistency ]//
        log_storage->updateMetaData(current_term, voted_for);
        //[ persistency ]//
        return;
    }
    if(role != RaftRole::Leader){
        // If this server has been not a leader anymore, it don't have ability to do anything about this reply.
        return;
    }
    if(reply.term > arg.term){
        // This means this snapshot is out of date
        return;
    }
    if(next_index[node_id] > arg.lastIncludedIndex){
        // This means this snapshot reply is out of date
        //! but we can predict that this is rare to happen
        //!debug//
        RAFT_LOG("A snapshot reply is out of date");
        //!debug//
        return;
    }
    match_index[node_id] = arg.lastIncludedIndex;
    next_index[node_id] = arg.lastIncludedIndex + 1;
    //!debug//
    //RAFT_LOG("receive snapshot reply from %d, match_index: %d, next_index: %d", node_id, match_index[node_id], next_index[node_id]);
    //!debug//
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr 
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
    RAFT_LOG("background election start");
    while (true) {
        {
            if (is_stopped()) {
                RAFT_LOG("background election stop");
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();
            // const auto MACHINE_NUM = rpc_clients_map.size();
            std::random_device rd;  
            std::mt19937 gen(rd()); 
            std::uniform_int_distribution<> dis1(300, 600);
            std::uniform_int_distribution<> dis2(1000, 1300);

            // const unsigned long TIMEOUT_LIMIT_FOLLOWER = ((300 * my_id) / MACHINE_NUM) + 300;
            const unsigned long TIMEOUT_LIMIT_FOLLOWER = dis1(gen);
            // const unsigned long TIMEOUT_LIMIT_CANDIDATE = (( 300 * my_id) / MACHINE_NUM) + 1000;
            const unsigned long TIMEOUT_LIMIT_CANDIDATE = dis2(gen);
            unsigned long current_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
            //!debug//
            //RAFT_LOG("Time from last receive: %lu ms", current_time - last_receive_timestamp);
            //!debug//
            if((role == RaftRole::Follower && current_time - last_receive_timestamp > TIMEOUT_LIMIT_FOLLOWER) || (role == RaftRole::Candidate && current_time - last_receive_timestamp > TIMEOUT_LIMIT_CANDIDATE)){
                //!debug//
                if(role != RaftRole::Candidate){
                    RAFT_LOG("I am candidate now");
                }
                //!debug
                role = RaftRole::Candidate;
                ++current_term;
                // By default, a candidate will definitely vote itself so it has a vote at the beginning
                received_votes = 1;
                voted_for = my_id;
                //!Don't forget to reset election timer
                last_receive_timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now()).time_since_epoch().count();
                for(auto map_it = rpc_clients_map.begin();map_it != rpc_clients_map.end();++map_it){
                    // Don't need to send heartbeat to itself
                    int target_id = map_it->first;
                    if(target_id == my_id){
                        continue;
                    }
                    RequestVoteArgs vote_request;
                    vote_request.term = current_term;
                    vote_request.candidateId = my_id;
                    // TODO: last log index & last log term
                    vote_request.lastLogIndex = log_entry_list[log_entry_list.size() - 1].index;
                    vote_request.lastLogTerm = log_entry_list[log_entry_list.size() - 1].term;
                    thread_pool->enqueue(&RaftNode::send_request_vote, this, target_id, vote_request);
                }
                //[ persistency ]//
                log_storage->updateMetaData(current_term, voted_for);
                //[ persistency ]//
            }
            mtx.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();
            if(role == RaftRole::Leader){
                for(auto map_it = rpc_clients_map.begin();map_it != rpc_clients_map.end();++map_it){
                    if(map_it->first == my_id){
                        continue;
                    }
                    // [used for no snapshot] //
                    // if(next_index[map_it->first] < log_entry_list.size()){
                    //     //!debug//
                    //     //RAFT_LOG("%d machine's next index is %d, log entry list size:%zu", map_it->first, next_index[map_it->first], log_entry_list.size());
                    //     //!debug//
                    //     std::vector<LogEntry<Command>> append_entry_list;
                    //     append_entry_list.clear();
                    //     for(int i = next_index[map_it->first];i < log_entry_list.size();++i){
                    //         append_entry_list.push_back(log_entry_list[i]);
                    //     }
                    //     AppendEntriesArgs<Command> args;
                    //     args.term = current_term;
                    //     args.leaderId = my_id;
                    //     args.prevLogIndex = next_index[map_it->first] - 1;
                    //     //!debug//
                    //     //RAFT_LOG("send request to %d node, prev log index: %d", map_it->first, args.prevLogIndex);
                    //     //!debug//
                    //     args.prevLogTerm = log_entry_list[args.prevLogIndex].term;
                    //     args.leaderCommit = commit_index;
                    //     //!debug//
                    //     //RAFT_LOG("Leader's commit_index: %d", commit_index);
                    //     //!debug//
                    //     args.logEntryList = append_entry_list;
                    //     thread_pool->enqueue(&RaftNode::send_append_entries, this, map_it->first, args);
                    // }
                    if(next_index[map_it->first] < physical2logic(log_entry_list.size())){
                        // use log entry list to update
                        if(logic2physical(next_index[map_it->first]) > 0){
                            std::vector<LogEntry<Command>> append_entry_list;
                            append_entry_list.clear();
                            for(int i = logic2physical(next_index[map_it->first]); i < log_entry_list.size();++i){
                                append_entry_list.push_back(log_entry_list[i]);
                            }
                            AppendEntriesArgs<Command> args;
                            args.term = current_term;
                            args.leaderId = my_id;
                            args.prevLogIndex = next_index[map_it->first] - 1;
                            args.prevLogTerm = log_entry_list[logic2physical(args.prevLogIndex)].term;
                            args.leaderCommit = commit_index;
                            args.logEntryList = append_entry_list;
                            thread_pool->enqueue(&RaftNode::send_append_entries, this, map_it->first, args);
                        }
                        // use snapshot to update
                        else{
                            InstallSnapshotArgs args;
                            args.term = current_term;
                            args.leaderId = my_id;
                            args.lastIncludedIndex = snapshot_last_index;
                            args.lastIncludedTerm = snapshot_last_term;
                            // Maybe we need to store a temp snapshot in memory
                            args.data = last_snapshot;
                            // These two attribute is fake, because we will send to follower in single use.
                            args.offset = 0;
                            args.done = true;
                            //!debug//
                            //RAFT_LOG("send snapshot to %d node, last included index: %d by commit", map_it->first, args.lastIncludedIndex);
                            //!debug//
                            thread_pool->enqueue(&RaftNode::send_install_snapshot, this, map_it->first, args);
                        }
                    }
                }
            }
            mtx.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();
            if(last_applied < commit_index){
                for(int i = logic2physical(last_applied + 1);i <= logic2physical(commit_index);++i){
                    state->apply_log(log_entry_list[i].command);
                }
                last_applied = commit_index;
            }
            mtx.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    RAFT_LOG("background ping start");
    while (true) {
        {
            if (is_stopped()) {
                RAFT_LOG("background ping end");
                return;
            }
            /* Lab3: Your code here */
            mtx.lock();
            if(role == RaftRole::Leader){
                // Send heartbeat to every Follower
                for(auto map_it = rpc_clients_map.begin();map_it != rpc_clients_map.end();++map_it){
                    // Don't need to send heartbeat to itself
                    int target_id = map_it->first;
                    if(target_id == my_id){
                        continue;
                    }
                    if(logic2physical(next_index[map_it->first]) > 0){
                        AppendEntriesArgs<Command> args;
                        args.term = current_term;
                        args.leaderId = my_id;
                        args.prevLogIndex = next_index[map_it->first] - 1;
                        args.prevLogTerm = log_entry_list[logic2physical(args.prevLogIndex)].term;
                        args.leaderCommit = commit_index;
                        args.logEntryList.clear();
                        thread_pool->enqueue(&RaftNode::send_append_entries, this, map_it->first, args);
                    }
                    // use snapshot to update
                    else{
                        InstallSnapshotArgs args;
                        args.term = current_term;
                        args.leaderId = my_id;
                        args.lastIncludedIndex = snapshot_last_index;
                        args.lastIncludedTerm = snapshot_last_term;
                        // Maybe we need to store a temp snapshot in memory
                        args.data = last_snapshot;
                        // These two attribute is fake, because we will send to follower in single use.
                        args.offset = 0;
                        args.done = true;
                        //!debug//
                        //RAFT_LOG("send snapshot to %d node, last included index: %d by heartbeat", map_it->first, args.lastIncludedIndex);
                        //!debug//
                        thread_pool->enqueue(&RaftNode::send_install_snapshot, this, map_it->first, args);
                    }
                }
            }
            mtx.unlock();
            // We choose heartbeat interval 300 ms. So every servers' timeout limit will range from 300ms~600ms
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }
    }
    return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot(); 
}

}