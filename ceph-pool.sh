#!/bin/bash
get_pool () {
    OUT=`ceph osd pool get $2 $1 | awk '{print $2}'`
    echo -ne "$OUT\t"
}
POOLS=`ceph osd pool ls | tr '\n' ' '`
HEADER="Pool\tSize\tCrush\tpg_num"
for POOL in $POOLS; do
    if [ $POOL == `cut -f1 -d' '<<< "$POOLS"` ];
        then echo -e "\033[1;32m" 
        echo -en $HEADER 
        echo -e "\033[0m"; fi
    echo -ne "$POOL\t\t"
    get_pool size $POOL
    get_pool crush_ruleset $POOL
    get_pool pg_num $POOL
    get_pool 
    echo 
done | column -t
