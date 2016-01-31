#!/bin/bash
OSDS=`grep "ceph/osd" /etc/mtab`
for OSD in `echo "$OSDS" | awk '{print $1}'`; do
    DISK_ID=$(find /dev/disk/by-id -lname *`echo "$OSD" | cut -d'/' -f3`  | grep ata)
    if ! grep $DISK_ID /etc/fstab > /dev/null; then 
        OSDS_TO_FSTAB+=`echo "$OSDS" | grep "$OSD" | sed "s|$OSD|$DISK_ID|g"`$'\n'; 
    fi
done
if [ ! -z "${OSDS_TO_FSTAB// }" ]; then 
    echo "$OSDS_TO_FSTAB" | sed '/^$/d' >> /etc/fstab; 
fi
