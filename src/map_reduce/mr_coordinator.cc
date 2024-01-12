#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce {
    std::tuple<int, int, std::string, int, int> Coordinator::askTask(int) {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        if(!isMapFinished){
            int index = -1;
            int i = 0;
            for(auto it = map_task_handout.begin();it != map_task_handout.end();++it, ++i){
                if(*it == false){
                    index = i;
                    break;
                }
            }
            if(index != -1){
                map_task_handout[index] = true;
                return std::tuple<int, int, std::string, int, int>(
                    static_cast<int>(mapReduce::mr_tasktype::MAP),
                    index,
                    files[index],
                    0,
                    0
                );
            }
            else{
                return std::tuple<int, int, std::string, int, int>(
                    static_cast<int>(mapReduce::mr_tasktype::NONE),
                    -1,
                    "",
                    0,
                    0
                );
            }
        }
        else if(!isReduceFinished){
            int index = -1;
            int i = 0;
            for(auto it = reduce_task_handout.begin();it != reduce_task_handout.end();++it, ++i){
                if(*it == false){
                    index = i;
                    break;
                }
            }
            if(index != -1){
                reduce_task_handout[index] = true;
                return std::tuple<int, int, std::string, int, int>(
                    static_cast<int>(mapReduce::mr_tasktype::REDUCE),
                    index,
                    "",
                    reduce_task_handout.size(),
                    files.size()
                );
            }
            else{
                return std::tuple<int, int, std::string, int, int>(
                    static_cast<int>(mapReduce::mr_tasktype::NONE),
                    -1,
                    "",
                    0,
                    0
                );
            }
        }
        else{
            if(!isMergeStart){
                isMergeStart = true;
                return std::tuple<int, int, std::string, int, int>(
                    static_cast<int>(mapReduce::mr_tasktype::NONE),
                    0,
                    "",
                    reduce_task_handout.size(),
                    files.size()
                );
            }
            else{
                return std::tuple<int, int, std::string, int, int>(
                    static_cast<int>(mapReduce::mr_tasktype::NONE),
                    -1,
                    "",
                    0,
                    0
                );
            }
        }
    }

    int Coordinator::submitTask(int taskType, int index) {
        // Lab4 : Your code goes here.
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        if(taskType == mapReduce::mr_tasktype::MAP){
            map_task_finish[index] = true;
            bool is_map_finished = true;
            for(auto it = map_task_finish.begin();it != map_task_finish.end();++it){
                if(*it == false){
                    is_map_finished = false;
                    break;
                }
            }
            isMapFinished = is_map_finished;
        }
        else if(taskType == mapReduce::mr_tasktype::REDUCE){
            reduce_task_finish[index] = true;
            bool is_reduce_finished = true;
            for(auto it = reduce_task_finish.begin();it != reduce_task_finish.end();++it){
                if(*it == false){
                    is_reduce_finished = false;
                    break;
                }
            }
            isReduceFinished = is_reduce_finished;
        }
        else{
            isFinished = true;
        }
        return 0;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
        // Lab4: Your code goes here (Optional).
        this->isMapFinished = false;
        this->isReduceFinished = false;
        this->isMergeStart = false;
        map_task_handout.clear();
        map_task_finish.clear();
        reduce_task_handout.clear();
        reduce_task_finish.clear();
        for(int i = 0;i < this->files.size();++i){
            map_task_handout.push_back(false);
            map_task_finish.push_back(false);
        }
        for(int i = 0;i < nReduce;++i){
            reduce_task_finish.push_back(false);
            reduce_task_handout.push_back(false);
        }
        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);
    }
}