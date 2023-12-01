# Lab 3

**Hand out: Nov 21, 2023** 

**Deadline: Dec 5 23:59, No Extension**

## Introduction

**Raft** is a consensus algorithm for replicated log.  Raft decomposes the consensus problem into relatively independent subproblems, which are much easier to understand. 
The key data structure in Raft is the **log**, which organizes the clients' requests into a sequence. 
Raft guarantees all the servers will **apply the same log commands in the same order**, which means the servers will all be in a consistent state. 
If a server fails but later recovers, Raft takes care of bringing its log up to date. 
And Raft can work as long as at least a majority of the servers are alive and connected.

Raft implements consensus by first electing a leader among the servers (part 1), then giving the leader authority and responsibility for managing the log. The leader accepts log entries from clients, replicates them on other servers, and tells servers when it is safe to apply log entries to their state machines (part 2). The logs should be persisted on the non-volatile storage to tolerate machine crashes (part 3). And as the log grows longer, Raft will compact the log via snapshotting (part 4).


This lab will follow the description of the Raft paper. **So, make sure you have read and understood the Raft paper of [the extended version](https://raft.github.io/raft.pdf) (especially section 5 and section 7) before coding.** And you will find that **Figure 2** and **Figure 13** in the raft paper can cover most of your design in this lab.

There are 4 parts in this lab.

* In part 1, you will implement the leader election and heartbeat mechanism of Raft.
* In part 2, you will implement the log replication protocol of Raft.
* In part 3, you will persist Raft log.
* In part 4, you will implement the snapshot mechanism of Raft.

Each part relies on the implementation of the prior one. So you must implement these parts one by one.

## Preparation

### Get the source code

- Pull the source code of lab3 from our GitLab:

```bash
cd chfs && git pull
```

- Change it to the lab3 branch

```bash
git checkout -b lab3 origin/lab3
```

- Merge with the code of lab2 and resolve any conflicts on your own if there are any.

```bash
git merge lab2
```

- Update submodules if needed:

```bash
git submodule update --init --recursive
```

### Compile Code And Test

Please also refer to [lab1.md](../lab1/lab1.md) for more details. It's the same with lab1.

## Overview of the code

You will mainly modify and complete the codes in `src/include/rsm/raft/node.h`, `src/include/rsm/raft/log.h`, `src/include/rsm/raft/protocol.h`.

There are 4 important C++ classes you need to pay attention to.

### `ChfsCommand` 

Class `ChfsCommand` in `src/include/rsm/state_machine.h` is related to the state machine. When the state machines append or apply a log, the ChfsCommand will be used. The state machines process identical sequences of ChfsCommand from the logs, so they produce the same outputs.

```c++
class ChfsCommand {
public:
    virtual ~ChfsCommand() {}

    virtual size_t size() const = 0;
    virtual std::vector<u8> serialize(int size) const = 0;
    virtual void deserialize(std::vector<u8> data, int size) = 0;
};

```

### `ChfsStateMachine`

Class `ChfsStateMachine` in `src/include/rsm/state_machine.h` represents the replicated state machines in Raft. We have already implemented `ChfsCommand` and `ChfsStateMachine`, but you still need to check the interfaces provided by them in the early parts. For example, you will use the `ChfsStateMachine::apply_log` interface to apply a committed Raft log to the state machine.

```c++
class ChfsStateMachine {
public:
    virtual ~ChfsStateMachine() {}

    /* Apply a log to the state machine. */
    virtual void apply_log(ChfsCommand &) = 0;

    /* Generate a snapshot of the current state. */
    virtual std::vector<u8> snapshot() = 0;
    
    /* Apply the snapshot to the state mahine. */
    virtual void apply_snapshot(const std::vector<u8> &) = 0;
};
```

### `RaftNode` 

The `RaftNode` class (in `src/include/rsm/raft/node.h`) is the core of your implementation, which represents a Raft node (or Raft server). `RaftNode` is a class template with two template parameters, `StateMachine` and `Command`. Remember we're implementing a raft library that decouples the consensus algorithm from the replicated state machine. Therefore, the user can implement their own state machine and pass it to the Raft library via the two template parameters. 

The user ensures the `StateMachine` inherits from `ChfsStateMachine` and the `Command` inherits from `ChfsCommand`. So you can use the interfaces provided by the two base classes in your implementation.

```c++
class RaftNode {
public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);

    ~RaftNode();

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
}
```

### `RaftLog`

The last important class is `RaftLog`, which you will complete to persist the Raft log and metadata. `RaftLog` is also a class template with a template parameter named `Command`, which is the same as the template parameter of `RaftNode` class. And you can use the interface provided by `ChfsCommand`, such as `size`, `deserialize` and `serialize` to implement the log persistency.

```c++
template<typename command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm);
}
```

**Notice: You must not change the constructor definition of these classes.**

## Understand the `RaftNode` class

Now, let's first walk through how `RaftNode` works. 

Our raft algorithm is implemented **asynchronously**, which means the events (e.g. leader election or log replication) should all happen in the background.

A `RaftNode` starts after `RaftNode::start()` is called, and it will create 4 background threads.

```c++
template<typename StateMachine, typename command>
void RaftNode<StateMachine, Command>::start() {
    RAFT_LOG("start");
    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);
    ...
}
```

The background threads will periodically do something in the background (e.g. send heartbeats in `run_background_ping`, or start an election in `run_background_election`).
And you will implement the body of these background threads.

Besides the events, the RPCs between `RaftNode` also should be sent and handled asynchronously. To implement an asynchronous RPC call, this lab provides a thread pool to handle asynchronous events.

### ThreadPool

Here shows a basic example of using `ThreadPool`.

```C++
#include "ThreadPool.h"

struct foo {
    int bar(int x) {
        return x*x;
    }
};

int main() {
    foo f;
    ThreadPool pool(4);
    auto result = pool.enqueue(&foo::bar, &f, 4); // (function pointer, this pointer, argument)
    return 0;	
}
```

### Marshall & unmarshall

LibRPC provides `MSGPACK_DEFINE` to automatically marshall and unmarshall custom data structures in RPC calls.

```C++
typedef struct {
  unsigned long ProtocolID;
  unsigned long RxStatus;
  unsigned long TxFlags;
  unsigned long Timestamp;
  unsigned long ExtraDataIndex;
  RPCLIB_MSGPACK::type::raw_ref DataRef;
  MSGPACK_DEFINE(
    ProtocolID,
    RxStatus,
    TxFlags,
    Timestamp,
    ExtraDataIndex,
    DataRef
  )
} RPC_PASSTHRU_MSG;
```

You may need to manually convert template types in custom data structure to basic types before you can use `MSGPACK_DEFINE`.


## Test

### Part 1 - Leader Election

In this part, you will implement the **leader election** protocol and **heartbeat** mechanism of the Raft consensus algorithm. And you can refer to **Figure 2** in the raft paper to implement this part.

You'd better follow the steps:

1. Complete the `RequestVoteArgs` and `RequestVoteReply` class in `protocol.h`. You can use `MSGPACK_DEFINE` to automatically marshall and unmarshall the structures for RPC.
2. Complete the method `RaftNode::request_vote` in `node.h` following Figure 2 in the Raft paper (you may also need to define some variables for the `RaftNode` class, such as commit_idx).
3. Complete the method `RaftNode::handle_request_vote_reply`, which should handle the RPC reply.
4. Complete the method `RaftNode::run_background_election`, which should turn to candidate and start an election after a leader timeout by sending request_vote RPCs asynchronously. 
5. Now, the raft nodes should be able to elect a leader automatically. But to keep its leadership, the leader should send heartbeat (i.e. an empty AppendEntries RPC) to the followers periodically. You can implement the heartbeat by implementing the AppendEntries RPC (e.g. complete `AppendEntriesArgs`, `AppendEntriesReply` in `protocal.h` and `RaftNode::append_entries`, `RaftNode::handle_append_entries_reply`, `RaftNode::run_background_ping` in `node.h`).
6. To pass the test, you need to implement three basic RPC interfaces provided by RaftNode (`RaftNode::start`, `RaftNode::is_leader`, `RaftNode::stop` in `node.h`).


You should pass the two test cases in RaftTestPart1:
* `RaftTestPart1.LeaderElection` (5)
* `RaftTestPart1.ReElection` (10)

**Hints**:

* Test:
  * You can run a single test case by editing `test/CMakeLists.txt`, and write the test cases you want to run after `TEST_FILTER`. For example, `RaftTestPart1.LeaderElection` for testing leader election and `RaftTestPart1*` for testing all the cases in part1.
* Debug:
  * We provide a macro in `node.h` named `RAFT_LOG`, you can use this macro to print the system log for debugging. The usage of this macro is the same as `printf`, e.g. `RAFT_LOG("Three is %d", 3);`. But it will provide additional information, such as node_id and term, in the console.
* Implementation:
  * Be careful about the election timeout, heartbeat period, and other time. You can refer to the paper. For example, make sure the election timeouts in different nodes don't always happen at the same time, otherwise all nodes will vote only for themselves and no one will become the leader.
  * You can send asynchronous RPC via `ThreadPool`.
  * Use the big lock (e.g. use `std::unique_lock<std::mutex> lock(mtx);` at the beginning of all the events) to avoid concurrent bugs.
  * Use a lock to protect RPC client pointers (also check null pointers before accessing them), as tests would set them to null intentionally.
  * The background threads should sleep some time after each loop iteration, instead of busy-waiting the event.
  * You don't have to implement all of the rules in AppendEntries RPC in this part (e.g. no need for a log). You only need to implement the heartbeat.
  * You don't have to worry about the persistence issue until part 3.



### Part 2 - Log Replication

In this part, you will implement the `log replication` protocol of the Raft consensus algorithm. Still, you can refer to Figure 2 in the raft paper.

Recommended steps:

1. Complete `RaftNode::new_command` in `node.h` to append new command to the leader's log.
2. Complete the methods related to the AppendEntries RPC.
3. Complete `RaftNode::run_background_commit` in `node.h` to send logs to the followers asynchronously.
4. Complete `RaftNode::run_background_apply` in `node.h` to apply the committed logs to the state machine.

You should pass the 7 test cases of RaftTestPart2:
* `RaftTestPart2.BasicAgree` (10)
* `RaftTestPart2.FailAgreement` (5)
* `RaftTestPart2.FailNoAgreement` (5)
* `RaftTestPart2.ConcurrentStarts` (5)
* `RaftTestPart2.Rejoin` (5)
* `RaftTestPart2.Backup` (5)
* `RaftTestPart2.RpcCount` (5)

Hints:

* Notice that the first log index is 1 instead of 0. To simplify the programming, you can append an empty log entry to the logs at the very beginning. And since the 'lastApplied' index starts from 0, the first empty log entry will never be applied to the state machine.
* Make sure your implementation is the same as the description of Figure 2 in the Raft paper. 
* Do yourself a favor for future labs (especially for lab 3 and lab 4). Make your code clean and readable.
* Remember to use the mutex!
* Don't forget to marshall and unmarshall custom data structures.
* The test cases may fail due to the bug from part 1.

### Part 3 - Log Persistency

In this part, you will persist the states of a Raft node. Check Figure 2 in the Raft paper again, to figure out what should be persisted.

Recommended steps:

1. You should implement the class `RaftLog` in `log.h` to persist the necessary states (e.g. logs) and use the constructor `RaftLog(std::shared_ptr<BlockManager> bm)` to create a `RaftLog` object in `RaftNode`. Each raft node will have its own logs to persist the states. And after a failure, the node will restore its storage via this dir.
2. Before every test, we will create an empty directory `/tmp/raft_log`, you can store logs there for convenience. (You can store logs anywhere you want as long as it works)
3. To ease the difficulty of your implementation, you can assume that total size of raft log is always smaller than 64K and size of a single log entry is always smaller than 4K.
4. You must not use C/C++ file operations to persist your log.
5. You should use the `RaftNode::log_storage` to persist the state, whenever they are changed.
6. And you should use the storage to restore the state when a Raft node is created.

You should pass the 6 test cases of RaftTestPart3:
* `RaftTestPart3.BasicPersist` (5)
* `RaftTestPart3.MorePersistence` (10)
* `RaftTestPart3.Persist3` (5)
* `RaftTestPart3.UnreliableAgree` (5)
* `RaftTestPart3.Figure8` (3)
* `RaftTestPart3.UnreliableFigure8` (2)

Hints:

* The test cases may fail due to the bugs from part 1 and part 2.
* To simplify your implementation, you don't have to consider the crash during the disk I/O. The test case won't crash your program during the I/O. For example, you don't have to make sure the atomicity of the state persists.
* You can use multiple files to persist different data (e.g. a file for metadata and the other for logs).
* To persist the command, you can use the `serialize` and `deserialize` interfaces of the `ChfsCommand`.

### Part 4 - Snapshot 

In this part, you will implement the snapshot mechanism of the Raft algorithm. You can refer to Figure 13 in the Raft extended paper. 

**Notice: You can send the whole snapshot in a single RPC.**

Recommended steps:

1. Complete the classes and methods related to `RaftNode::install_snapshot`.
2. Complete the method `RaftNode::save_snapshot`.
3. Modify all the codes related to the log you have implemented before. (E.g. number of logs)
4. Restore the snapshot in the raft constructor.

You should pass the 3 test cases of RaftTestPart4:
* `RaftTestPart4.BasicSnapshot` (5)
* `RaftTestPart4.RestoreSnapshot` (5)
* `RaftTestPart4.OverrideSnapshot` (5)

Hints:

* To make the code clear, you can use two concepts for the log index: physical index (e.g. the index of the `std::vector`) and logical index (e.g. physical index + snapshot index). 
* This part may introduce many changes to your code base. So you'd better commit your codes before this part. 

## Handin 

Execute the following command under `scripts/lab3` directory:

```bash
./handin.sh
```

Then you will see a `handin.tgz` file under the root directory of this project. Please rename it in the format of: `lab3_[your student id].tgz`, and upload this `.tgz` file to Canvas.