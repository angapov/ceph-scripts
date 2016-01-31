#!/bin/bash
DISKS=`fdisk -l 2> /dev/null| grep "Disk /dev/sd" | awk '{print $2}' | cut -d: -f1`

disks() {
for DISK in $DISKS; do
    if hdparm -I $DISK 2>/dev/null 1>&2 && hdparm -I $DISK 2>/dev/null | grep "Model Number" | grep -q SSD
        then TYPE=SSD; else TYPE=SATA 
    fi
    for MOUNT in `mount | grep ceph | awk '{print $1}'`; do
        if [[ "$MOUNT" =~ "$DISK" ]]; then 
            USED_FOR=`mount | grep $MOUNT | awk '{print $3}'`
            JOURNAL=`realpath $USED_FOR/journal`
            JOURNAL_SIZE=$((`fdisk -s $JOURNAL`/1024/1024))
            USED_FOR=${USED_FOR##/var/lib/ceph/osd/}
            echo -e "$DISK\t$TYPE\t$USED_FOR\t$JOURNAL\t$JOURNAL_SIZE"
            continue
        fi
    done
done | column -t
}

journal() {
    for DISK in `find /dev -regextype posix-egrep -regex '/dev/sd[a-z]|/dev/nvme[0-1]n[0-1]'`; do
        if hdparm -I $DISK 2>/dev/null | grep "Model Number" | grep -q SSD || [[ "$DISK" =~ "/dev/nvme" ]]
            then 
		echo "------------------------------------"
		echo -e "\033[1;36m${DISK##/dev/}\033[0m\n($((`fdisk -s $DISK`/1024/1024)) GB)"
	    else continue
	    fi
	for PARTITION in `ls "$DISK"*`; do
	    if [ "$DISK" == "$PARTITION" ]; then continue; fi
	    SIZE=$((`fdisk -s $PARTITION`/1024/1024))
	    echo -ne "\033[36m  ${PARTITION##/dev/}\033[0m ("$SIZE" GB)\t"
	    RES=0
            for MOUNT in `mount | grep ceph | awk '{print $3}'`; do
                JOURNAL=`realpath "$MOUNT/journal"`
                if [[ "$JOURNAL" == "$PARTITION" ]]; then
 		            RES=1
                    echo -e "${MOUNT##/var/lib/ceph/osd/}"
		            continue 2
                fi
            done
	    if [ $RES -eq 0 ]; 
            then 
                if grep -q ${PARTITION##/dev/} /proc/mdstat; then
                    echo /dev/`grep ${PARTITION##/dev/} /proc/mdstat | awk '{print $1}'`
                else echo ---
                fi
        fi
        done
    done 
}
case $1 in
    d) disks;;
    j) journal;;
    *) echo "Usage: d - show disks, j - show journals"
esac
