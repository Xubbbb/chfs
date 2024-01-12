#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
        auto look_res = chfs_client->lookup(1, outPutFile);
        if(look_res.is_err()){
            std::cout << "Output file didn't create!" << std::endl;
            auto create_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, outPutFile);
            if(create_res.is_err()){
                std::cout << "create failed!" << std::endl;
            }
            return;
        }
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here
        std::vector<KeyVal> all_map_vec;
        all_map_vec.clear();

        for(auto file : files){
            auto look_res = chfs_client->lookup(1, file);
            if(look_res.is_err()){
                std::cout << "ERROR2" << std::endl;
                return;
            }
            auto file_inode = look_res.unwrap();
            auto res_type = chfs_client->get_type_attr(file_inode);
            auto length = res_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(file_inode, 0, length);
            auto char_vec = res_read.unwrap();
            std::string content(char_vec.begin(), char_vec.end());
            auto map_res = Map(content);
            all_map_vec.insert(all_map_vec.end(), map_res.begin(), map_res.end());
        }

        int offset = 0;
        auto out_inode = chfs_client->lookup(1, outPutFile).unwrap();
        while(!all_map_vec.empty()){
            auto new_key = all_map_vec[0].key;
            std::vector<std::string> key_val_vec;
            key_val_vec.clear();
            for(auto it = all_map_vec.begin();it != all_map_vec.end();++it){
                if((*it).key == new_key){
                    key_val_vec.push_back((*it).val);
                    all_map_vec.erase(it);
                    --it;
                }
            }
            auto reduce_res = Reduce(new_key, key_val_vec);
            auto write_content = new_key + " " + reduce_res + " ";
            auto write_vec = std::vector<chfs::u8>(write_content.begin(), write_content.end());
            auto write_res = chfs_client->write_file(out_inode, offset, write_vec);
            if(write_res.is_ok()){
                offset += write_vec.size();
            }
        }
    }
}