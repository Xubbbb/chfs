#!/bin/bash

ROOT_PATH=/home/stu/chfs

tar_file="$ROOT_PATH/handin.tgz"

tar -czvf "$tar_file" -C "$(dirname "$ROOT_PATH")" "$(basename "$ROOT_PATH")"/daemons "$(basename "$ROOT_PATH")"/src
