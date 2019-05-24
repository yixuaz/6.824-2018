#!/bin/bash
#TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B
#TestPersistPartitionUnreliable3A
#TestPersistPartitionUnreliableLinearizable3A
export GOPATH="/home/zyx/Desktop/mit6.824/6.824"
export PATH="$PATH:/usr/lib/go-1.9/bin"

rm res2 -rf
mkdir res2
set int j = 0
for ((i = 0; j < 25; i++))
do
    for ((c = $((i*1)); c < $(( (i+1)*1)); c++))
    do
         (go test TestPersistPartitionUnreliableLinearizable3A) &> ./res2/$c
    done

    #sleep 450

    if grep -nr "FAIL.*raft.*" res2; then
        echo "fail"
    fi
done