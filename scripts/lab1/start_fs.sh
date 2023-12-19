#!/bin/bash

PARENT_DIR="$(dirname "$(realpath "$0")")"
source $PARENT_DIR/set-env.sh

ulimit -c unlimited

#CHFSDIR1=$ROOT_PATH/mnt
CHFSDIR1=/tmp/mnt

rm -rf $CHFSDIR1
mkdir $CHFSDIR1 || exit 1
sleep 1
echo "Starting chfs"
/home/stu/chfs/build/bin/fs $CHFSDIR1 > $ROOT_PATH/chfs.log 2>&1 &

sleep 5

# make sure FUSE is mounted where we expect
if [ `mount | grep "$CHFSDIR1" | grep -v grep | wc -l` -ne 1 ]; then
    $PARENT_DIR/stop_fs.sh
    echo "Failed to mount chfs properly"
    exit -1
fi