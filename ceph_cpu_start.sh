#!/bin/bash
echo $$ > ceph_cpu.pid
for host in localhost slpeah001; do
    > cpu.log.$host
done
TS0=`date +%s`
declare -A CPU=()
declare -A MEM=()
while true; do
    FIOJOB=`ps ax | grep fio | grep -- '--name=' | sed 's|^.*--name=\(.*\) --rwmix.*$|\1|'` 
    TS1=`date +%s`
    SEC=`echo "$TS1-$TS0" |bc`
    CPU_local=`pidstat -C ceph-osd 2 1 | grep Average | grep -v PID | awk '{sum+=\$7} END {print sum}'`
    MEM_local=`pidstat -C ceph-osd -r | tail -n+4 | awk '{sum+=\$8} END {print sum}'`
    if [ -z "$CPU_local" ]; then CPU_local=0; fi
    echo -e $SEC"\t"$CPU_local"\t"$MEM_local"\t"$FIOJOB >> cpu.log.localhost
    for host in slpeah001; do 
        CPU[$host]=$(ssh $host "pidstat -C ceph-osd 2 1" | grep Average | grep -v PID | awk '{sum+=$7} END {print sum}')
        MEM[$host]=$(ssh $host "pidstat -C ceph-osd -r"  | tail -n+4 | awk '{sum+=$8} END {print sum}')
        if [ -z "${CPU[$host]}" ]; then CPU[$host]=0; fi
        echo -e $SEC"\t"${CPU[$host]}"\t"${MEM[$host]}"\t"$FIOJOB >> cpu.log.$host
    done
done
