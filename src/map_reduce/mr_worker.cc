#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

#include "map_reduce/protocol.h"

namespace mapReduce {

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    void Worker::doMap(int index, const std::string &filename) {
        // Lab4: Your code goes here.
        auto look_res = chfs_client->lookup(1, filename);
        auto inode_id = look_res.unwrap();
        auto res_type = chfs_client->get_type_attr(inode_id);
        auto length = res_type.unwrap().second.size;
        auto res_read = chfs_client->read_file(inode_id, 0, length);
        auto char_vec = res_read.unwrap();
        std::string content(char_vec.begin(), char_vec.end());
        auto map_res = Map(content);
        auto mknode_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(index));
        auto inter_inode = mknode_res.unwrap();
        std::stringstream str_stream("");
        for(auto &key_val : map_res){
            str_stream << key_val.key << " " << key_val.val << " ";
        }
        auto str = str_stream.str();
        std::vector<chfs::u8> write_content_vec(str.begin(), str.end());
        auto write_res = chfs_client->write_file(inter_inode, 0, write_content_vec);
        if(write_res.is_err()){
            std::cout << "write failed!" << std::endl;
        }
        doSubmit(mapReduce::mr_tasktype::MAP, index);
    }

    void Worker::doReduce(int index, int nfiles, int nreduces) {
        // Lab4: Your code goes here.
        int begin = (26 / nreduces) * index;
        int end;
        if(index == nreduces - 1){
            end = 26;
        }
        else{
            end = begin + (26 / nreduces);
        }
        std::vector<KeyVal> reduce_target;
        reduce_target.clear();
        auto mknode_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "reduce-" + std::to_string(index));
        auto inter_inode = mknode_res.unwrap();

        for(int i = 0;i < nfiles;++i){
            auto look_res = chfs_client->lookup(1, "mr-" + std::to_string(i));
            auto inode_id = look_res.unwrap();
            auto res_type = chfs_client->get_type_attr(inode_id);
            auto length = res_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(inode_id, 0, length);
            auto char_vec = res_read.unwrap();
            std::string content(char_vec.begin(), char_vec.end());
            std::stringstream str_stream(content);
            std::string key, value;
            while(str_stream >> key >> value){
                if(!key.empty() && ((key[0] >= ('a' + begin) && key[0] < ('a' + end)) || (key[0] >= ('A' + begin) && key[0] < ('A' + end)))){
                    reduce_target.push_back(KeyVal(key, value));
                }
            }
        }

        std::string write_content = "";
        while(!reduce_target.empty()){
            auto new_key = reduce_target[0].key;
            std::vector<std::string> key_val_vec;
            key_val_vec.clear();
            for(auto it = reduce_target.begin();it != reduce_target.end();++it){
                if((*it).key == new_key){
                    key_val_vec.push_back((*it).val);
                    reduce_target.erase(it);
                    --it;
                }
            }
            auto reduce_res = Reduce(new_key, key_val_vec);
            write_content.append(new_key + " " + reduce_res + " ");
        }
        std::cout << index << " reduce task: " << write_content.substr(0, 100) << std::endl;
        auto write_vec = std::vector<chfs::u8>(write_content.begin(), write_content.end());
        auto write_res = chfs_client->write_file(inter_inode, 0, write_vec);
        if(write_res.is_ok()){
            doSubmit(mapReduce::mr_tasktype::REDUCE, index);
        }
    }

    void Worker::doMerge(int nReduce){
        auto out_look_res = chfs_client->lookup(1, outPutFile);
        auto out_inode = out_look_res.unwrap();
        int offset = 0;
        for(int i = 0;i < nReduce;++i){
            auto look_res = chfs_client->lookup(1, "reduce-" + std::to_string(i));
            auto inode_id = look_res.unwrap();
            auto res_type = chfs_client->get_type_attr(inode_id);
            auto length = res_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(inode_id, 0, length);
            auto char_vec = res_read.unwrap();
            auto write_res = chfs_client->write_file(out_inode, offset, char_vec);
            if(write_res.is_ok()){
                offset += char_vec.size();
            }
        }
        doSubmit(mapReduce::mr_tasktype::NONE, 0);
    }

    void Worker::doSubmit(mr_tasktype taskType, int index) {
        // Lab4: Your code goes here.
        mr_client->call(SUBMIT_TASK, static_cast<int>(taskType), index);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            // Lab4: Your code goes here.
            auto ask_task_res = mr_client->call(ASK_TASK, 0);
            if(ask_task_res.is_err()){
                std::cout << "A client call ask task error!" << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            else{
                auto res_tuple = ask_task_res.unwrap()->as<std::tuple<int, int, std::string, int, int>>();
                int type = std::get<0>(res_tuple);
                if(type == mapReduce::mr_tasktype::NONE){
                    int nreduces = std::get<3> (res_tuple);
                    int nfiles = std::get<4>(res_tuple);
                    if(nreduces > 0 && nfiles > 0){
                        doMerge(nreduces);
                    }
                    else{
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    }
                }
                else if(type == mapReduce::mr_tasktype::MAP){
                    int index = std::get<1>(res_tuple);
                    std::string filename = std::get<2>(res_tuple);
                    // std::cout << "A worker get " << index << " map task to handle " << filename << std::endl;
                    doMap(index, filename);
                }
                else{
                    int index = std::get<1>(res_tuple);
                    int nreduces = std::get<3> (res_tuple);
                    int nfiles = std::get<4>(res_tuple);
                    doReduce(index, nfiles, nreduces);
                }
            }
        }
    }
}