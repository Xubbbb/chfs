#!/bin/bash

PARENT_DIR="$(dirname "$(realpath "$0")")"
source $PARENT_DIR/set-env.sh

#CHFSDIR1=$ROOT_PATH/mnt
CHFSDIR1=/tmp/mnt
SCRIPTS_PATH=$ROOT_PATH/scripts/lab1

passed_cnt=0

$SCRIPTS_PATH/stop_fs.sh
sleep 2
$SCRIPTS_PATH/start_fs.sh

test_chsf_alive(){
	ps -e | grep -q "fs"
	if [ $? -ne 0 ];
	then
		echo "FATAL: chfs process died"
		$SCRIPTS_PATH/stop_fs.sh
		exit
	fi;
}
test_chsf_alive

test_chsf_mounted(){
	mount | grep -q "$CHFSDIR1"
	if [ $? -ne 0 ];
	then
		echo "FATAL: chfs not mounted"
		$SCRIPTS_PATH/stop_fs.sh
		exit
	fi;
}
test_chsf_mounted

##################################################

test_part2_a(){
	$SCRIPTS_PATH/test-lab1-part2-a.pl $CHFSDIR1 | grep -q "Passed part2 A"
	if [ $? -ne 0 ];
	then
	    echo "Failed part2 A"
	else
	    passed_cnt=$((passed_cnt+1))
		echo "Passed part2 A"
	fi
	test_chsf_alive
	test_chsf_mounted
}

##################################################

test_part2_b(){
	$SCRIPTS_PATH/test-lab1-part2-b.pl $CHFSDIR1 | grep -q "Passed part2 B"
	if [ $? -ne 0 ];
	then
	    echo "Failed part2 B"
	else
	    passed_cnt=$((passed_cnt+1))
		echo "Passed part2 B"
	fi
	test_chsf_alive
	test_chsf_mounted
}

##################################################

test_part2_c(){
	$SCRIPTS_PATH/test-lab1-part2-c.pl $CHFSDIR1 | grep -q "Passed part2 C"
	if [ $? -ne 0 ];
	then
	    echo "Failed part2 C"
	else
	    passed_cnt=$((passed_cnt+1))
		echo "Passed part2 C"
	fi
	test_chsf_alive
	test_chsf_mounted
}

##################################################

test_part2_d(){
	$SCRIPTS_PATH/test-lab1-part2-d.sh $CHFSDIR1 | grep -q "Passed part2 D"
	if [ $? -ne 0 ];
	then
	    echo "Failed part2 D"
	else
	    passed_cnt=$((passed_cnt+1))
		echo "Passed part2 D"
	fi
	test_chsf_alive
	test_chsf_mounted
}

##################################################

test_part2_e(){
	$SCRIPTS_PATH/test-lab1-part2-e.sh $CHFSDIR1 | grep -q "Passed part2 E"
	if [ $? -ne 0 ];
	then
	    echo "Failed part2 E"
	else
	    passed_cnt=$((passed_cnt+1))
		echo "Passed part2 E"
	fi
	test_chsf_alive
	test_chsf_mounted
}

##################################################

if [ $# -eq 1 ]; then
	if [ "$1" = "A" ]; then
		test_part2_a	
	elif [ "$1" = "B" ]; then
		test_part2_b
	elif [ "$1" = "C" ]; then
		test_part2_c
	elif [ "$1" = "D" ]; then
		test_part2_d
	elif [ "$1" = "E" ]; then
		test_part2_e
	else
		echo "Unknown Part "$1" "
	fi
	$SCRIPTS_PATH/stop_fs.sh
else
	test_part2_a	
	test_part2_b
	test_part2_c
	test_part2_d
	test_part2_e
	$SCRIPTS_PATH/stop_fs.sh
	echo ""
	echo "Passed "$passed_cnt"/5 tests"
fi