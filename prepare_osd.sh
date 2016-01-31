#!/bin/bash
OSDS=`fdisk -l 2>/dev/null | grep 6001.2 | awk '{print $2}' | cut -d: -f1`
i=1
for OSD in $OSDS; do
   JOURNAL=`sed -n ${i}p journals.txt`
   echo -e "o\ny\nw\ny\n" | gdisk $OSD > /dev/null
   ceph-disk prepare $OSD $JOURNAL
   ceph-disk activate ${OSD}1
   ((i++))
done
