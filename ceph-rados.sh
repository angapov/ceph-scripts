#!/bin/bash
POOL="rbd"
IMAGE="superimage"
PREFIX=`rbd info $IMAGE | grep block_name_prefix | awk '{print $2}'`
for i in `rados -p $POOL ls | grep $PREFIX`; do
    rados -p $POOL stat $i
done
