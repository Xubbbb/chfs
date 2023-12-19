#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

template<typename Command>
struct LogEntry{
    int index;
    int term;
    Command command;
};

struct RequestVoteArgs {
    /* Lab3: Your code here */
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
    
    MSGPACK_DEFINE(
        term,
        candidateId,
        lastLogIndex,
        lastLogTerm
    )
};

struct RequestVoteReply {
    /* Lab3: Your code here */
    int term;
    bool voteGranted;
    int voteFollowerId;

    MSGPACK_DEFINE(
        term,
        voteGranted,
        voteFollowerId
    )
};

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    int leaderCommit;

    std::vector<LogEntry<Command>> logEntryList;
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    int leaderCommit;

    std::vector<int> logEntryIndexList;
    std::vector<int> logEntryTermList;
    std::vector<std::vector<u8>> logEntryCommandList;
    
    MSGPACK_DEFINE(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        leaderCommit,
        logEntryIndexList,
        logEntryTermList,
        logEntryCommandList
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */
    RpcAppendEntriesArgs transformed;
    transformed.term = arg.term;
    transformed.leaderId = arg.leaderId;
    transformed.prevLogIndex = arg.prevLogIndex;
    transformed.prevLogTerm = arg.prevLogTerm;
    transformed.leaderCommit = arg.leaderCommit;
    transformed.logEntryIndexList.clear();
    transformed.logEntryTermList.clear();
    transformed.logEntryCommandList.clear();
    for(const LogEntry<Command> logEntry : arg.logEntryList){
        transformed.logEntryIndexList.push_back(logEntry.index);
        transformed.logEntryTermList.push_back(logEntry.term);
        int command_size = logEntry.command.size();
        std::vector<u8> serialized = logEntry.command.serialize(command_size);
        transformed.logEntryCommandList.push_back(serialized);
    }
    return transformed;
}


template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    AppendEntriesArgs<Command> transformed;
    transformed.term = rpc_arg.term;
    transformed.leaderId = rpc_arg.leaderId;
    transformed.prevLogIndex = rpc_arg.prevLogIndex;
    transformed.prevLogTerm = rpc_arg.prevLogTerm;
    transformed.leaderCommit = rpc_arg.leaderCommit;
    transformed.logEntryList.clear();
    auto index_it = rpc_arg.logEntryIndexList.begin();
    auto end_flag = rpc_arg.logEntryIndexList.end();
    auto term_it = rpc_arg.logEntryTermList.begin();
    auto command_it = rpc_arg.logEntryCommandList.begin();
    for(;index_it != end_flag;++index_it, ++term_it, ++command_it){
        LogEntry<Command> logEntry;
        logEntry.index = *index_it;
        logEntry.term = *term_it;
        Command command;
        int command_size = command.size();
        command.deserialize((*command_it), command_size);
        logEntry.command = command;
        transformed.logEntryList.push_back(logEntry);
    }
    return transformed;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    int term;
    bool success;

    MSGPACK_DEFINE(
        term,
        success
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */
    int term;
    int leaderId;
    int lastIncludedIndex;
    int lastIncludedTerm;
    int offset;
    std::vector<u8> data;
    bool done;

    MSGPACK_DEFINE(
        term,
        leaderId,
        lastIncludedIndex,
        lastIncludedTerm,
        offset,
        data,
        done
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */
    int term;
    MSGPACK_DEFINE(
        term
    )
};

} /* namespace chfs */