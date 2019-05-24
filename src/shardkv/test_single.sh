#!/bin/bash

export GOPATH="/home/zyx/Desktop/mit6.824/6.824"
export PATH="$PATH:/usr/lib/go-1.9/bin"

rm res -rf
mkdir res
#set int j = 0
for ((i = 0; i < 30; i++))
do
    for ((c = $((i*10)); c < $(( (i+1)*10)); c++))
    do
         (go test -run TestChallenge2Unaffected) &> ./res/$c &
    done

    sleep 22
    grep -IRiL "PASS.*" res
   # grep -nr "FAIL.*" res

done