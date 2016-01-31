#!/bin/bash
SSDS=`fdisk -l 2>/dev/null | grep 200.0 | awk '{print $2}' | cut -d: -f1`
NVMES='/dev/nvme0n1  /dev/nvme1n1'
secure_erase() {
    echo "======= STARTING SECURITY ERASE ==============="
    echo "This script will TOTALLY DESTROY ALL DATA on disks:"
    echo $SSDS | tr '\n' ' '; echo
    echo -n "Agree? [y/n]: "
    read ANS
    if ! [ $ANS == 'y' ]; then exit 1; fi
    for DISK in $SSDS; do 
        echo "Performing secure erase of $DISK..."
        hdparm --user-master u --security-set-pass Eins $DISK > /dev/null
        hdparm --user-master u --security-erase Eins $DISK > /dev/null
    done
}
write_new_GPT() {
    echo -e "o\ny\nw\ny\n" | gdisk $1 > /dev/null
}
create_partitions() {
    NUM=$1
    SIZE=$2
    DEVICE=$3
    write_new_GPT $DEVICE
    for i in `seq $NUM`; do
        echo "Preparing partition $i on $DEVICE, size $SIZE GB"
        echo -e "n\n\n\n+${SIZE}GB\n\nw\ny\n" | gdisk $DEVICE > /dev/null
    done
}
secure_erase
> journals.txt
for DISK in $NVMES; do
    create_partitions 3 40 $DISK
    ls ${DISK}?? >> journals.txt
done
#for DISK in $SSDS; do
#    create_partitions 3 40 $DISK
#    ls ${DISK}? >> journals.txt
#done
