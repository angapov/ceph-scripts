#!/usr/bin/env python
import rbd
import rados
import json
import subprocess
from itertools import chain
from texttable import Texttable, get_color_string, bcolors

def f(x):
    if x=="quota_max_bytes":
        return str(pool[x]/1024/1024)
    else:
        return str(pool[x])

p = subprocess.check_output('ceph osd dump -f json-pretty', shell=True)
pools       = json.loads(p)['pools']
pools_table = Texttable()
header      = [ "Id", "Pool", "Size", "Min_size", "Pg_num", "Pgp_num", "Crush","Quota (MB)", "Quota (obj)" ]
keys        = [ "pool", "pool_name", "size", "min_size", "pg_num", "pg_placement_num", "crush_ruleset","quota_max_bytes","quota_max_objects" ]
pools_table.header(map(lambda x: get_color_string(bcolors.YELLOW, x), header))
for pool in pools:
    pools_table.add_row(map(f, keys))

table = Texttable()
table.set_deco(Texttable.BORDER | Texttable.HEADER | Texttable.VLINES)
table.set_cols_align( [ "l", "l", "l", "l", "l", "l", "l" ])
table.set_cols_valign([ "m", "m", "m", "m", "m", "m", "m" ])
table.set_cols_width([ "20", "20", "8","8","20","8","8"])
header = [ "Pool", "Image", "Size(Mb)", "Features", "Lockers", "Str_size", "Str_cnt" ]
keys   = [ "features", "list_lockers", "stripe_unit", "stripe_count" ]
table.header(map(lambda x: get_color_string(bcolors.YELLOW, x), header))

with rados.Rados(conffile='/etc/ceph/ceph.conf') as cluster:
    pool_list = cluster.list_pools()
    for pool in pool_list:
        table.add_row([  get_color_string(bcolors.GREEN, pool) , "", "", "", "", "", "" ])
        with cluster.open_ioctx(pool) as ioctx:
            rbd_inst = rbd.RBD()
            image_list = rbd_inst.list(ioctx)
            for image_name in image_list:
                with rbd.Image(ioctx, image_name) as image:
                    image_size = str(image.size()/1024**2)
                    table.add_row(["", image_name, image_size] + map(lambda x: str(getattr(image,x)()), keys))
        if pool != pool_list[-1]:
            table.add_row([ "-"*20, "-"*20,"-"*8,"-"*8,"-"*20,"-"*8,"-"*8 ])

print(pools_table.draw())
print
print(table.draw())
