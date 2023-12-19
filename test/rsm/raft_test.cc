#include "raft_test.h"

#include <thread>

namespace chfs{

class RaftTestDebug: public RaftTest {};

class RaftTestPart1: public RaftTest {};

TEST_F(RaftTestPart1, LeaderElection)
{
  InitNodes(3);

  mssleep(300);
  EXPECT_GE(CheckOneLeader(), 0);

  mssleep(200);
  int term1 = CheckSameTerm();

  mssleep(200);
  int term2 = CheckSameTerm();

  EXPECT_EQ(term1, term2) << "inconsistent term";
  EXPECT_GE(CheckOneLeader(), 0);
}

TEST_F(RaftTestPart1, ReElection)
{
  InitNodes(5);

  /* 1. check one leader */
  mssleep(300);
  int leader1 = CheckOneLeader();

  /* 2. stop the leader */
  DisableNode(leader1);
  mssleep(1000);

  int leader2 = CheckOneLeader();
  EXPECT_EQ(leader1 != leader2, true) << "node " << leader2 << " shouldn't be the new leader";

  /* 3. stop the second leader */
  DisableNode(leader2);
  mssleep(1000);

  int leader3 = CheckOneLeader();
  EXPECT_EQ(leader1 != leader3 && leader3 != leader2, true) << "node " << leader3 << " shouldn't be the new leader";

  /* 4. stop the third leader */
  DisableNode(leader3);
  mssleep(1000);

  /* 5. only 2 nodes left with no leader */
  CheckNoLeader();

  /* 6. resume a node */
  EnableNode(leader1);
  mssleep(1000);
  CheckOneLeader();
}

class RaftTestPart2: public RaftTest {};

TEST_F(RaftTestPart2, BasicAgree)
{
  int node_num = 3;

  InitNodes(node_num);

  for (int i = 1; i < 1 + node_num; i++) {
    int num_committed = NumCommitted(i);
    ASSERT_EQ(num_committed, 0) << "The log " << i << "should not be committed!";

    int log_idx = AppendNewCommand(i * 100, node_num);
    ASSERT_EQ(log_idx, i) << "got index " << log_idx << ", but expect " << i;
  }
}

TEST_F(RaftTestPart2, FailAgreement)
{
  int node_num = 3;

  InitNodes(node_num);
  mssleep(300);

  ASSERT_GE(AppendNewCommand(101, node_num), 0);
  
  int leader = CheckOneLeader();
  DisableNode(leader);

  ASSERT_GE(AppendNewCommand(102, node_num - 1), 0);
  ASSERT_GE(AppendNewCommand(103, node_num - 1), 0);
  ASSERT_GE(AppendNewCommand(104, node_num - 1), 0);
  ASSERT_GE(AppendNewCommand(105, node_num - 1), 0);

  EnableNode(leader);

  ASSERT_GE(AppendNewCommand(106, node_num), 0);
  ASSERT_GE(AppendNewCommand(107, node_num), 0);
}

TEST_F(RaftTestPart2, FailNoAgreement)
{
  int node_num = 5;

  InitNodes(node_num);
  mssleep(300);

  ASSERT_GE(AppendNewCommand(10, node_num), 0);

  int leader = CheckOneLeader();
  ASSERT_GE(leader, 0);
  DisableNode((leader + 1) % node_num);
  DisableNode((leader + 2) % node_num);
  DisableNode((leader + 3) % node_num);

  mssleep(500);
  
  bool is_leader;
  int temp_term, temp_index;

  ListCommand cmd1(20);
  auto res1 = clients[leader]->call(RAFT_RPC_NEW_COMMEND, cmd1.serialize(cmd1.size()), cmd1.size());
  std::tie(is_leader, temp_term, temp_index) = res1.unwrap()->as<std::tuple<bool, int, int>>();
  ASSERT_TRUE(is_leader);
  ASSERT_EQ(temp_index, 2);

  mssleep(2000);

  int num_committed = NumCommitted(temp_index);
  ASSERT_EQ(num_committed, 0) << "There is no majority, but index " << temp_index << " is committed";

  EnableNode((leader + 1) % node_num);
  EnableNode((leader + 2) % node_num);
  EnableNode((leader + 3) % node_num);

  mssleep(500);

  int leader2 = CheckOneLeader();
  ASSERT_GE(leader2, 0);

  ListCommand cmd2(30);
  auto res2 = clients[leader2]->call(RAFT_RPC_NEW_COMMEND, cmd2.serialize(cmd2.size()), cmd2.size());
  std::tie(is_leader, temp_term, temp_index) = res2.unwrap()->as<std::tuple<bool, int, int>>();
  ASSERT_TRUE(is_leader);
  ASSERT_TRUE(temp_index == 2 || temp_index == 3);

  ASSERT_GE(AppendNewCommand(1000, node_num), 0);
}

TEST_F(RaftTestPart2, ConcurrentStarts)
{
  int node_num = 3;

  InitNodes(3);

  bool success;

  for (int tries = 0; tries < 5; tries++) {
    if (tries > 0) {
      mssleep(3000);
    }

    int leader = CheckOneLeader();
    ASSERT_GE(leader, 0);

    bool is_leader;
    int term, index;

    ListCommand cmd1(1);
    auto res1 = clients[leader]->call(RAFT_RPC_NEW_COMMEND, cmd1.serialize(cmd1.size()), cmd1.size());
    std::tie(is_leader, term, index) = res1.unwrap()->as<std::tuple<bool, int, int>>();
    if (!is_leader) 
      continue;
    
    int iters = 5;
    std::mutex mtx;
    std::vector<int> indices;
    std::vector<std::unique_ptr<std::thread>> threads;
    bool failed = false;
    std::vector<int> values;

    for (int i = 0; i < iters; i++) {
      threads.push_back(std::make_unique<std::thread>(
        [&] (int j) {
          ListCommand temp_cmd(100 + j);

          bool temp_is_leader;
          int temp_term, temp_index;
          auto temp_res = clients[leader]->call(RAFT_RPC_NEW_COMMEND, temp_cmd.serialize(temp_cmd.size()), temp_cmd.size());
          std::tie(temp_is_leader, temp_term, temp_index) = temp_res.unwrap()->as<std::tuple<bool, int, int>>();

          if (!temp_is_leader)
            return;
          if (term != temp_term)
            return;
          
          mtx.lock();
          indices.push_back(temp_index);
          mtx.unlock();
        }, 
        i
      ));
    }

    for (int i = 0; i < iters; i++) {
      threads[i]->join();
    }

    for (int i = 0; i < node_num; i++) {
      auto temp_res = clients[i]->call(RAFT_RPC_CHECK_LEADER);
      if (std::get<1>(temp_res.unwrap()->as<std::tuple<bool, int>>()) != term)
        goto loop_end;
    }

    for (auto index: indices) {
      int res = WaitCommit(index, node_num, term);
      if (res == -1) {
        failed = true;
        break;
      }
      values.push_back(res);
    }

    if (failed)
      continue;

    for (int i = 0; i < iters; i++) {
      int ans = i + 100;
      bool find = false;
      for (auto value : values) {
        if (value == ans)
          find = true;
      }
      EXPECT_TRUE(find) << "cmd " << ans << " missing";
    }

    success = true;
    break;

  loop_end:
    continue;
  }

  EXPECT_TRUE(success) << "term change too often";
}

TEST_F(RaftTestPart2, Rejoin)
{
  int node_num = 3;
  InitNodes(3);

  ASSERT_GE(AppendNewCommand(101, node_num), 0);
  
  int leader1 = CheckOneLeader();
  ASSERT_GE(leader1, 0);

  /* leader stop working */
  DisableNode(leader1);

  ListCommand cmd1(102);
  ListCommand cmd2(103);
  ListCommand cmd3(104);
  clients[leader1]->call(RAFT_RPC_NEW_COMMEND, cmd1.serialize(cmd1.size()), cmd1.size());
  clients[leader1]->call(RAFT_RPC_NEW_COMMEND, cmd2.serialize(cmd2.size()), cmd2.size());
  clients[leader1]->call(RAFT_RPC_NEW_COMMEND, cmd3.serialize(cmd3.size()), cmd3.size()); 

  int leader2 = CheckOneLeader();
  ASSERT_GE(leader2, 0);
  ASSERT_GE(AppendNewCommand(103, node_num - 1), 0);

  DisableNode(leader2);

  EnableNode(leader1);

  ASSERT_GE(AppendNewCommand(104, node_num - 1), 0);

  EnableNode(leader2);

  ASSERT_GE(AppendNewCommand(105, node_num), 0);
}

TEST_F(RaftTestPart2, Backup)
{
  int node_num = 5;
  InitNodes(5);

  int value = 0;
  ASSERT_GE(AppendNewCommand(value++, node_num), 0);

  /* put leader and one follower in a partition */
  int leader1 = CheckOneLeader();
  ASSERT_GE(leader1, 0);
  DisableNode((leader1 + 2) % node_num);
  DisableNode((leader1 + 3) % node_num);
  DisableNode((leader1 + 4) % node_num);

  /* submit lots of commands that won't commit */
  for (int i = 0; i < 50; i++) {
    ListCommand cmd(value++);
    clients[leader1]->call(RAFT_RPC_NEW_COMMEND, cmd.serialize(cmd.size()), cmd.size());
  }

  mssleep(500);

  DisableNode(leader1);
  DisableNode((leader1 + 1) % node_num);

  /* allow other partition to recover */
  EnableNode((leader1 + 2) % node_num);
  EnableNode((leader1 + 3) % node_num);
  EnableNode((leader1 + 4) % node_num); 


  /* lots of successful commands to new group. */
  for (int i = 0; i < 50; i++) {
    ASSERT_GE(AppendNewCommand(value++, node_num - 2), 0);
  }

  /* now another partitioned leader and one follower */
  int leader2 = CheckOneLeader();
  ASSERT_GE(leader2, 0);
  int other = (leader1 + 2) % node_num;
  if (other == leader2) {
    other = (leader2 + 1) % node_num;
  }

  DisableNode(other);

  /* lots more commands that won't commit */
  for (int i = 0; i < 50; i++) {
    ListCommand cmd(value++);
    clients[leader2]->call(RAFT_RPC_NEW_COMMEND, cmd.serialize(cmd.size()), cmd.size());
  }

  mssleep(500);

  /* bring original leader back to life */
  for (int i = 0; i < node_num; i++) {
    DisableNode(i);
  }
  EnableNode(leader1 % node_num);
  EnableNode((leader1 + 1) % node_num);
  EnableNode(other);

  /* lots of successful commands to new group. */
  for (int i = 0; i < 50; i++) {
    ASSERT_GE(AppendNewCommand(value++, node_num - 2), 0);
  }

  /* now everyone */
  for (int i = 0; i < node_num; i++) {
    EnableNode(i);
  }

  ASSERT_GE(AppendNewCommand(value++, node_num), 0);
}

/* RPC counts aren't too high */
TEST_F(RaftTestPart2, RpcCount)
{
  const auto processor_count = std::thread::hardware_concurrency();

  int node_num = 3;
  InitNodes(3);

  int leader = CheckOneLeader();
  ASSERT_GE(leader, 0);

  int total1 = RpcCount(-1);

  EXPECT_TRUE(total1 <= 30 && total1 >= 1) << "too many or few RPCs (" << total1 << ") to elect initial leader";

  bool success = false;
  int total2;
  bool failed;
  for (int tries = 0; tries < 5; tries++) {
    int value = 1;
    if (tries > 0)
      mssleep(3000);
    
    leader = CheckOneLeader();
    ASSERT_GE(leader, 0);
    total1 = RpcCount(-1);

    bool is_leader;
    int term, index;
    ListCommand cmd1(1024);
    auto res1 = clients[leader]->call(RAFT_RPC_NEW_COMMEND, cmd1.serialize(cmd1.size()), cmd1.size());
    std::tie(is_leader, term, index) = res1.unwrap()->as<std::tuple<bool, int, int>>();
    if (!is_leader)
      continue;

    int iters = 10;
    int rpc_limit = iters * 10;
    if (processor_count && processor_count < 4) {
      rpc_limit = iters * (24 / processor_count + 15);
    }
    for (int i = 1; i < iters + 2; i++)
    {
      int term1, index1;
      ListCommand cmd(value);
      auto res2 = clients[leader]->call(RAFT_RPC_NEW_COMMEND, cmd.serialize(cmd.size()), cmd.size());
      std::tie(is_leader, term1, index1) = res2.unwrap()->as<std::tuple<bool, int, int>>();
      if (!is_leader)
        goto loop_end;
      if (term1 != term)
        goto loop_end;
      value++;
      ASSERT_EQ(index + i, index1) << "wrong commit index " << index1 << ", expected " << index + i;
    }

    for (int i = 1; i < iters + 1; i++) {
      int res = WaitCommit(index + i, node_num, term);
      if (res == -1)
        goto loop_end;
      ASSERT_TRUE(res == i) << "wrong value " << res << " committed for index "
                            << index + i << "; expected " << i;
    }

    failed = false;
    for (int j = 0; j < node_num; j++) {
      auto res3 = clients[j]->call(RAFT_RPC_CHECK_LEADER);

      int temp_term = std::get<1>(res3.unwrap()->as<std::tuple<bool, int>>());
      if (temp_term != term)
      {
        failed = true;
      }
    }
    if (failed)
      goto loop_end;

    total2 = RpcCount(-1);
    EXPECT_TRUE(total2 - total1 <= rpc_limit) << "too many RPCs (" << total2 - total1 << ") for " << iters
                                                        << " entries" << ", limit is " << rpc_limit;
    success = true;
    break;
  loop_end:
    continue;
  }

  ASSERT_TRUE(success) << "term changed too often";

  total2 = RpcCount(-1);
  mssleep(1000);

  int total3 = RpcCount(-1);
  EXPECT_TRUE(total3 - total2 <= 50) << "too many RPCs (" << total3 - total2 << ") for 1 second of idleness";
}

class RaftTestPart3: public RaftTest 
{
public:
  void Figure8Test(int num_tries = 1000)
  {
    int num_nodes = 5;
    ASSERT_GE(AppendNewCommand(2048, 1), 0);
    int nup = num_nodes;
    for (int iters = 0; iters < num_tries; iters++)
    {
      int leader = -1;
      for (int i = 0; i < num_nodes; i++) {

        ListCommand cmd(iters);
        auto res = clients[i]->call(RAFT_RPC_NEW_COMMEND, cmd.serialize(cmd.size()), cmd.size());
        bool is_leader = std::get<0>(res.unwrap()->as<std::tuple<bool, int, int>>());
        if (is_leader) {
          leader = i;
        }
      }

      if (rand() % 1000 < 100) {
        mssleep(rand() % 500);
      } else {
        mssleep(rand() % 13);
      }

      if (leader != -1) {
        DisableNode(leader);
        nup--;
      }
      if (nup < 3)
      {
        int s = rand() % num_nodes;
        if (!node_network_available[s])
        {
          Restart(s);
          nup++;
        }
      }
    }

    for (int i = 0; i < num_nodes; i++)
    {
      Restart(i);
    }

    ASSERT_GE(AppendNewCommand(1024, num_nodes), 0);
  }
};

TEST_F(RaftTestPart3, BasicPersist)
{
  int num_nodes = 3;
  InitNodes(num_nodes);
  mssleep(100);

  ASSERT_GE(AppendNewCommand(11, num_nodes), 0);

  for (int i = 0; i < num_nodes; i++) {
    Restart(i);
  }

  for (int i = 0; i < num_nodes; i++) {
    DisableNode(i);
    EnableNode(i);
  }

  mssleep(100);

  ASSERT_GE(AppendNewCommand(12, num_nodes), 0);

  ASSERT_EQ(GetCommittedValue(1), 11);

  int leader1 = CheckOneLeader();
  Restart(leader1);

  ASSERT_GE(AppendNewCommand(13, num_nodes), 0);

  int leader2 = CheckOneLeader();
  ASSERT_GE(leader2, 0);
  DisableNode(leader2);
  ASSERT_GE(AppendNewCommand(14, num_nodes - 1), 0);
  Restart(leader2);

  ASSERT_GE(WaitCommit(4, num_nodes, -1), 0); // wait for leader2 to join before killing i3

  int i3 = (CheckOneLeader() + 1) % num_nodes;
  DisableNode(i3);
  ASSERT_GE(AppendNewCommand(15, num_nodes - 1), 0);
  Restart(i3);

  ASSERT_GE(AppendNewCommand(16, num_nodes), 0);
}

TEST_F(RaftTestPart3, MorePersistence)
{
  int num_nodes = 5;
  InitNodes(num_nodes);

  int index = 1;
  for (int iters = 0; iters < 5; iters++) {
    ASSERT_GE(AppendNewCommand(10 + index, num_nodes), 0);
    index++;

    int leader1 = CheckOneLeader();
    ASSERT_GE(leader1, 0);

    DisableNode((leader1 + 1) % num_nodes);
    DisableNode((leader1 + 2) % num_nodes);

    ASSERT_GE(AppendNewCommand(10 + index, num_nodes - 2), 0);
    index++;

    DisableNode((leader1 + 0) % num_nodes);
    DisableNode((leader1 + 3) % num_nodes);
    DisableNode((leader1 + 4) % num_nodes);

    Restart((leader1 + 1) % num_nodes);
    Restart((leader1 + 2) % num_nodes);

    mssleep(1000);

    Restart((leader1 + 3) % num_nodes);

    ASSERT_GE(AppendNewCommand(10 + index, num_nodes - 2), 0);
    index++;

    EnableNode((leader1 + 0) % num_nodes);
    EnableNode((leader1 + 4) % num_nodes);
  }
  ASSERT_GE(AppendNewCommand(1000, num_nodes), 0);
  ASSERT_GE(WaitCommit(index, num_nodes, -1), 0);
}

TEST_F(RaftTestPart3, Persist3)
{
  int num_nodes = 3;
  InitNodes(num_nodes);

  ASSERT_GE(AppendNewCommand(101, num_nodes), 0);

  int leader = CheckOneLeader();
  ASSERT_GE(leader, 0);
  DisableNode((leader + 2) % num_nodes);

  ASSERT_GE(AppendNewCommand(102, num_nodes - 1), 0);

  DisableNode((leader + 0) % num_nodes);
  DisableNode((leader + 1) % num_nodes);

  EnableNode((leader + 2) % num_nodes);

  Restart((leader + 0) % num_nodes);

  ASSERT_GE(AppendNewCommand(103, num_nodes - 1), 0);

  Restart((leader + 1) % num_nodes);
  ASSERT_GE(AppendNewCommand(104, num_nodes), 0);

  ASSERT_GE(WaitCommit(4, num_nodes, -1), 0);
}

/* Agreement under unreliable network */
TEST_F(RaftTestPart3, UnreliableAgree)
{
  int num_nodes = 5;
  InitNodes(num_nodes);

  SetReliable(false);

  std::vector<std::unique_ptr<std::thread>> worker_theards;
  for (int iters = 1; iters < 50; iters++)
  {
    for (int j = 0; j < 4; j++)
    {
      worker_theards.push_back(
        std::make_unique<std::thread>(std::bind(&RaftTestPart3::AppendNewCommand, this, 100 * iters + j, 1))
      );
    }

    ASSERT_GE(AppendNewCommand(iters, 1), 0);
  }
  
  for (auto &&worker: worker_theards) {
    worker->join();
  }

  ASSERT_GE(AppendNewCommand(4096, num_nodes), 0);
}

TEST_F(RaftTestPart3, Figure8)
{
  int num_nodes = 5;
  InitNodes(num_nodes);

  Figure8Test();
}

TEST_F(RaftTestPart3, UnreliableFigure8)
{
  int num_nodes = 5;
  InitNodes(num_nodes);

  SetReliable(false);

  Figure8Test(100);
}

class RaftTestPart4: public RaftTest {};

TEST_F(RaftTestPart4, BasicSnapshot)
{
  int num_nodes = 3;
  InitNodes(num_nodes);
  
  int leader = CheckOneLeader();
  ASSERT_GE(leader, 0);
  int killed_node = (leader + 1) % num_nodes;
  DisableNode(killed_node);

  for (int i = 1; i < 100; i++)
    ASSERT_GE(AppendNewCommand(100 + i, num_nodes - 1), 0);
  
  auto res0 = clients[leader]->call(RAFT_RPC_GET_SNAPSHOT);
  std::vector<u8> snapshot0 = res0.unwrap()->as<std::vector<u8>>();

  ListStateMachine tmp_sm;
  tmp_sm.apply_snapshot(snapshot0);

  ASSERT_EQ(tmp_sm.store.size(), 100);
  for (int i = 1; i < 100; i++) {
    ASSERT_EQ(tmp_sm.store[i], i + 100);
  }

  leader = CheckOneLeader();
  ASSERT_GE(leader, 0);
  int other_node = (leader + 1) % num_nodes;
  if (other_node == killed_node)
    other_node = (leader + 2) % num_nodes;

  auto res1 = clients[leader]->call(RAFT_RPC_SAVE_SNAPSHOT);
  ASSERT_TRUE(res1.unwrap()->as<bool>()) << "leader cannot save snapshot";

  auto res2 = clients[other_node]->call(RAFT_RPC_SAVE_SNAPSHOT);
  ASSERT_TRUE(res2.unwrap()->as<bool>()) << "follower cannot save snapshot";
  mssleep(2000);
  EnableNode(killed_node);
  leader = CheckOneLeader();
  ASSERT_GE(leader, 0);
  ASSERT_GE(AppendNewCommand(1024, num_nodes), 0);

  ASSERT_TRUE(nodes[killed_node]->get_list_state_log_num() < 90) << "the snapshot does not work";
}

TEST_F(RaftTestPart4, RestoreSnapshot)
{
  int num_nodes = 3;
  InitNodes(num_nodes);

  int leader = CheckOneLeader();
  ASSERT_GE(leader, 0);
  
  for (int i = 1; i < 100; i++)
    ASSERT_GE(AppendNewCommand(100 + i, num_nodes - 1), 0);
  
  leader = CheckOneLeader();
  ASSERT_GE(leader, 0);
  auto res1 = clients[leader]->call(RAFT_RPC_SAVE_SNAPSHOT);
  ASSERT_TRUE(res1.unwrap()->as<bool>()) << "leader cannot save snapshot";
  mssleep(2000);
  Restart(leader);
  ASSERT_GE(AppendNewCommand(1024, num_nodes), 0);
  ASSERT_TRUE(nodes[leader]->get_list_state_log_num() < 90) << "the snapshot does not work";
}

TEST_F(RaftTestPart4, OverrideSnapshot)
{
  int num_nodes = 3;
  InitNodes(num_nodes);

  int leader = CheckOneLeader();
  ASSERT_GE(leader, 0);

  for (int i = 1; i < 30; i++)
    ASSERT_GE(AppendNewCommand(100 + i, num_nodes - 1), 0);

  leader = CheckOneLeader();
  ASSERT_GE(leader, 0);

  auto res1 = clients[leader]->call(RAFT_RPC_SAVE_SNAPSHOT);
  ASSERT_TRUE(res1.unwrap()->as<bool>()) << "leader cannot save snapshot";
 
  for (int i = 30; i < 60; i++)
    ASSERT_GE(AppendNewCommand(100 + i, num_nodes - 1), 0);
  
  leader = CheckOneLeader();
  ASSERT_GE(leader, 0);

  auto res2 = clients[leader]->call(RAFT_RPC_SAVE_SNAPSHOT);
  ASSERT_TRUE(res2.unwrap()->as<bool>()) <<  "leader cannot save snapshot";
  mssleep(2000);
  Restart(leader);
  ASSERT_GE(AppendNewCommand(1024, num_nodes), 0);
  ASSERT_TRUE(nodes[leader]->get_list_state_log_num() < 50) << "the snapshot does not work";
}

} /* namespace chfs */
