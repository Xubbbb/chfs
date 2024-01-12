#include <string>
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include <regex>

#include "map_reduce/protocol.h"

namespace mapReduce{
//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
    std::vector<KeyVal> Map(const std::string &content) {
        // Your code goes here
        // Hints: split contents into an array of words.
        std::vector<KeyVal> ret;
        ret.clear();

        /**
         * We will use C++ std::regex this library to
         * help us use regular expressions to parse words
        */
        std::regex word_regex("([a-zA-Z]+)");
        auto words_begin = std::sregex_iterator(content.begin(), content.end(), word_regex);
        auto words_end = std::sregex_iterator();

        for (std::sregex_iterator i = words_begin; i != words_end; ++i) {
            std::smatch match = *i;

            bool is_exist = false;
            for(auto &key_val : ret){
                if(key_val.key == match.str()){
                    is_exist = true;
                    key_val.val = std::to_string(std::stoi(key_val.val) + 1);
                }
            }
            if(!is_exist){
                ret.push_back(KeyVal(match.str(), std::to_string(1)));
            }
        }

        return ret;
    }

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
    std::string Reduce(const std::string &key, const std::vector<std::string> &values) {
        // Your code goes here
        // Hints: return the number of occurrences of the word.
        std::string ret = "0";
        for(auto value : values){
            auto ret_int = std::stoi(ret);
            auto value_int = std::stoi(value);
            ret = std::to_string(ret_int + value_int);
        }
        return ret;
    }
}