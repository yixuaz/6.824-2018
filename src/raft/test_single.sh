#!/bin/bash

export GOPATH="/home/zyx/Desktop/mit6.824/6.824"
export PATH="$PATH:/usr/lib/go-1.9/bin"

rm res -rf
mkdir res
set int j = 0
for ((i = 0; j < 25; i++))
do
    if (($((i % 10)) == 0)); then
        rm res -rf
        mkdir res
        echo $i
    fi
    for ((c = $((i*30)); c < $(( (i+1)*30)); c++))
    do
         (go test -run TestBasicAgree2B) &> ./res/$c &
    done

    sleep 3
    if grep -nr "FAIL.*raft.*" res; then
        exit 1
    fi

done