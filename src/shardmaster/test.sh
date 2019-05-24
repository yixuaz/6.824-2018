#!/bin/bash

export GOPATH="/home/zyx/Desktop/mit6.824/6.824"
export PATH="$PATH:/usr/lib/go-1.9/bin"

rm res -rf
mkdir res

for ((i = 0; i < 100; i++))
do

    for ((c = $((i*5)); c < $(( (i+1)*5)); c++))
    do
         (go test -race) &> ./res/$c &
         sleep 2
    done

    sleep 10
    if grep -nr "WARNING.*" res; then
        echo "WARNING: DATA RACE"
    fi
    if grep -nr "FAIL.*" res; then
        echo "found fail"
    fi

done



