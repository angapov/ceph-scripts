#!/usr/bin/env python
NUM_OSD = [ 10, 20, 30, 40, 50, 100, 200, 500 ]
NUM_HOSTS = 10
DISK_FAIL_PROB_PER_YEAR = 0.0087
DISK_SIZE_GB = 6000
NETWORK_SPEED_Gbps = 1

for num_osd in NUM_OSD:
    NUM_PG = num_osd*100/3
    COMBINATIONS = num_osd*(num_osd-1)*(num_osd-2)
    HOSTS_CORRECTION_FACTOR = (num_osd - num_osd/float(NUM_HOSTS))*(num_osd - num_osd/float(NUM_HOSTS))/(num_osd-1)/(num_osd-2)
    HOSTS_CORRECTION_FACTOR = 1 if HOSTS_CORRECTION_FACTOR>1 else HOSTS_CORRECTION_FACTOR
    RECOVERY_TIME_HOURS = 3*DISK_SIZE_GB*3/float(NETWORK_SPEED_Gbps)*8/float(3600)
    SLA = 100 - 100*((DISK_FAIL_PROB_PER_YEAR*num_osd/float(365*24/float(RECOVERY_TIME_HOURS)))**3)*HOSTS_CORRECTION_FACTOR*NUM_PG/float(COMBINATIONS) 
    print "OSDs: %d\tSLA: %0.10f" % (num_osd, SLA) + "%"
