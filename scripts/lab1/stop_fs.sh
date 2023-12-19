#!/bin/bash

PARENT_DIR="$(dirname "$(realpath "$0")")"
source $PARENT_DIR/set-env.sh

#CHFSDIR1=$ROOT_PATH/mnt
CHFSDIR1=/tmp/mnt

export PATH=$PATH:/usr/local/bin
UMOUNT="umount"
if [ -f "/usr/local/bin/fusermount" -o -f "/usr/bin/fusermount" -o -f "/bin/fusermount" ]; then
    UMOUNT="fusermount -u";
fi
$UMOUNT $CHFSDIR1