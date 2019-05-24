#!/bin/bash

export GOPATH="/home/zyx/Desktop/mit6.824/6.824"
export PATH="$PATH:/usr/lib/go-1.9/bin"

rm res -rf
mkdir res
for ((i = 0; i < 200; i++))
do
echo $i
(go test) > ./res/$i
grep -nr "FAIL.*" res
done