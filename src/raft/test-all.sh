#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'exit 1' INT

# runs=$1
# for i in $(seq 1 $runs); do
#     timeout -k 2s 900s go test -run 2A | tee -a 2A.tmplog &
#     pid=$!
#     if ! wait $pid; then
#         echo '***' FAILED TESTS IN TRIAL $i
#         exit 1
#     fi
# done
# echo '***' PASSED ALL $i TESTING TRIALS FOR 2A

runs=$1
for i in $(seq 1 $runs); do
    echo "$i / $1"
    LOG="2A_$i.tmplog"
    VERBOSE=0 go test -run 2A &> $LOG
    # if ! wait $pid; then
    #     echo '***' FAILED TESTS IN TRIAL $i
    #     # exit 1
    # fi
    if [[ $? -ne 0 ]]; then
        echo "Failed at iter $i, saving log at $LOG"
        continue
    fi
    if [[ `cat $LOG | grep FAIL | wc -l` -gt 0 ]]; then
        echo "FAILED at iter $i, saving log at $LOG"
        continue
    fi
    rm $LOG
done
echo '***' PASSED ALL $i TESTING TRIALS 2B

for i in $(seq 1 $runs); do
    echo "$i / $1"
    LOG="2B_$i.tmplog"
    VERBOSE=0 go test -run 2B &> $LOG
    # if ! wait $pid; then
    #     echo '***' FAILED TESTS IN TRIAL $i
    #     # exit 1
    # fi
    if [[ $? -ne 0 ]]; then
        echo "Failed at iter $i, saving log at $LOG"
        continue
    fi
    if [[ `cat $LOG | grep FAIL | wc -l` -gt 0 ]]; then
        echo "FAILED at iter $i, saving log at $LOG"
        continue
    fi
    rm $LOG
done
echo '***' PASSED ALL $i TESTING TRIALS 2B
