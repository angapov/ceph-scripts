#!/usr/bin/python -u
from __future__ import print_function
import os
import sys
import libvirt
import rados
import rbd
import shutil
import subprocess
import argparse
import re
import ConfigParser
import logging as LOG
import signal
import multiprocessing
import traceback
from libvirt_qemu import qemuAgentCommand 
from novaclient import client as novaclient
from cinderclient.v2 import client as cinderclient
from keystoneclient import session
from keystoneclient.auth.identity import v3
from keystoneclient.v3 import client as keystoneclient
from pprint import pprint
from time import localtime, strftime, strptime, sleep
from glob import glob
from fabric import api as fabric
from datetime import datetime

# Time format used for naming backup and snapshot
TIME_FORMAT = '%Y-%m-%d-%H-%M'
USER_TIME_FORMAT = '%d-%m-%Y %H:%M'

# Constants related to backup list output formatting
INSTANCE_LEN    = 40
TENANT_LEN      = 40
DATE_LEN        = 18
TYPE_LEN        = 5
SIZE_LEN        = 9
STATUS_LEN      = 7
d               = "| "
header          = "+".join(("",
                  "-"*(INSTANCE_LEN+1),  \
                  "-"*(TENANT_LEN+1),    \
                  "-"*(DATE_LEN+1),      \
                  "-"*(TYPE_LEN+1),      \
                  "-"*(SIZE_LEN+1),      \
                  "-"*(STATUS_LEN+1),    \
                  ""))

# Terminal colors
YELLOW  = '\033[33m'
GREEN   = '\033[92m'
RED     = '\033[31m'
BROWN   = '\033[43m'
END     = '\033[0m'

# Backup status
STATUS_OK = 'OK'
STATUS_ERROR = 'ERROR'

def parse_args_and_config():
    config = ConfigParser.ConfigParser()
    conf_parser = argparse.ArgumentParser(add_help=False, 
                                            description=__doc__,
                                            formatter_class=argparse.RawDescriptionHelpFormatter)
    conf_parser.add_argument("-c", "--config",
                        default=default_conf,
                        dest='config',
                        help="Config file for backup daemon (default: /etc/ceph-backup.conf)")
    args, remaining_argv = conf_parser.parse_known_args()
    config.read(args.config)
    parser = argparse.ArgumentParser(parents=[conf_parser])
    group = parser.add_mutually_exclusive_group()
    group.add_argument( "-b", 
                        choices=['full', 'inc'],
                        dest='backup_type',
                        help="Backup type: full (full backup) or inc (incremental)")
    group.add_argument( "-l",
                        dest='list_backups',
                        action='store_true',
                        default=False,
                        help="List available backups for instances")
    group.add_argument( "-r",
                        dest='restore_date',
                        help="Restore list of instances inplace to given date (backup will"
                             " replace the existing instances)")
    parser.add_argument("-i",
                        dest='instances',
                        nargs="+",
                        default='', 
                        help="Comma separated list of instances (IDs or names) to backup")
    parser.add_argument("--with-root-disks",
                        dest='backup_root_disks',
                        action='store_true',
                        default=False,
                        help="Backup root disks of instances also (default: False)")
    args = parser.parse_args(remaining_argv)
    return args, config

def check_directory_is_writeable(dir):
    if os.access(dir, os.W_OK):
        with open(os.path.join(dir, 'test'), "w+") as f:
            try:
                f.write('test')
            except IOError:
                msg = "%s directory is not writeable. Check free space or permissions!" % dir
                LOG.exception(msg)
                raise sys.exit(msg)
            finally:
                try:
                    os.remove(os.path.join(dir, 'test'))
                except:
                    pass
    else:
        LOG.exception("No write access to %s. Check permissions." % dir)
        raise sys.exit(1)

def looks_like_date(string, time_format=TIME_FORMAT):
    try:
        time = strptime(string, time_format)
        return string
    except ValueError:
        return None

def looks_like_uuid(string):
    re_uuid = re.compile("[0-F]{8}-[0-F]{4}-[0-F]{4}-[0-F]{4}-[0-F]{12}", re.I)
    re_id = re.compile("[a-z0-9]+", re.I)
    like_uuid = len(string)==36 and bool(re_uuid.match(str(string)))
    like_id = len(string)==32 and bool(re_id.match(str(string)))
    return True if like_uuid or like_id else False

def get_keystone_session():
    auth = v3.Password( auth_url=OS_AUTH_URL,
                        username=OS_USERNAME,
                        password=OS_PASSWORD,
                        project_name=OS_PROJECT_NAME,
                        user_domain_name=USER_DOMAIN_NAME,
                        project_domain_name=PROJECT_DOMAIN_NAME)
    context = session.Session(auth=auth)
    client = keystoneclient.Client(session=context)
    return context, client

def instance_is_running(instance):
    instance = nova.servers.get(instance.id)
    if getattr(instance, 'OS-EXT-STS:power_state') == 1 and \
        instance.status == 'ACTIVE':
        return True
    else:
        return False
  
def execute(cmd, host=None):
    if host:
        return execute_remote_cmd(cmd, host)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = p.communicate()
    if err:
        out += str(err)
    rc = p.returncode
    return (out, rc)

def execute_remote_cmd(cmd, host):
    with fabric.settings(fabric.hide('warnings', 'running', 'stdout', 'stderr'),
                            host_string=host,
                            key_filename=SSH_KEY,
                            user=SSH_USER,
                            warn_only=True):
        out = fabric.run(cmd)
    return (out, out.return_code)

def detect_pool(rbd_name):
    if rbd_name.endswith('_disk') or rbd_name.endswith('_disk.bak'):
        return VMS_POOL
    elif rbd_name.startswith('volume-'):
        return VOLUMES_POOL

def get_pool_ioctx(rbd_name):
    pool = detect_pool(rbd_name)
    return VMS_POOL_IOCTX if pool==VMS_POOL else VOLUMES_POOL_IOCTX

def get_rbd_image_obj(rbd_name):
    ioctx = get_pool_ioctx(rbd_name)
    return rbd.Image(ioctx, rbd_name)

def delete_rbd_by_name(rbd_name):
    ioctx = get_pool_ioctx(rbd_name) 
    rbd_inst = rbd.RBD()
    with rbd.Image(ioctx, rbd_name) as rbd_img:
        remove_all_snapshots([rbd_img])
    rbd_inst.remove(ioctx, rbd_name)

def rename_rbd(rbd_name, new_name):
    ioctx = get_pool_ioctx(rbd_name)
    rbd_inst = rbd.RBD()
    rbd_inst.rename(ioctx, rbd_name, new_name)

def rbd_image_exists(rbd_name):
    ioctx = get_pool_ioctx(rbd_name)
    rbd_inst = rbd.RBD()
    return True if rbd_name in rbd_inst.list(ioctx) else False

def freeze_vm(dom):
    ret = dom.fsFreeze()

def thaw_vm(dom):
    ret = dom.fsThaw()

def rbd_snap_create(rbd_name, snap_name):
    ioctx = get_pool_ioctx(rbd_name)
    with rbd.Image(ioctx, rbd_name) as rbd_img:
        rbd_img.flush()
        rbd_img.create_snap(snap_name)

def exec_with_timeout(func, args=(), timeout=60):
    p = multiprocessing.Process(target=func, args=args)
    p.start()
    p.join(timeout)
    if p.is_alive():
        LOG.error("ERROR!!! Timed out waiting for snapshot completion!")
        p.terminate()
        p.join()

def exec_with_timeout2(func, args=(), kwargs={}, timeout_duration=20, default=None):
    def handler(signum, frame):
        raise Exception("ERROR ERROR ERROR!!! Timed out waiting for snapshot completion")

    signal.signal(signal.SIGALRM, handler) 
    signal.alarm(timeout_duration)
    try:
        func(*args, **kwargs)
    except Exception as msg:
        LOG.exception(msg)
    finally:
        signal.alarm(0)

def take_simple_snapshots(rbd_list, instance_name):
    curr_time = current_time()
    LOG.info("Taking RBD snapshots of %s" % instance_name)
    for rbd_image in rbd_list:
        rbd_name = "%s/%s" % (detect_pool(rbd_image.name), rbd_image.name)
        sleep(5)
        time1 = datetime.now()
        LOG.info("-- Snapshoting %s (+0.00 sec)" % rbd_name)
        rbd_image.flush()
        exec_with_timeout(rbd_image.create_snap, (curr_time,))
        time2 = datetime.now()
        LOG.info("-- Done snapshoting %s (+%s sec)" % \
                (rbd_name, timedelta(time2, time1)))

def take_rbd_snapshots(dom, rbd_list, instance_name):
    curr_time = current_time()
    if dom.isActive() and guest_agent_available(dom):
        try:
            time1 = datetime.now()
            LOG.info("Freezing %s (+0.00000 sec)" % instance_name)
            freeze_vm(dom)
            sleep(2)
            LOG.info("Taking RBD snapshots of %s (+%s sec)" \
                    % (instance_name, timedelta(datetime.now(), time1)))
            for rbd_image in rbd_list:
                rbd_name = "%s/%s" % (detect_pool(rbd_image.name), rbd_image.name)
                sleep(2)
                LOG.info("-- Snapshoting %s" % rbd_name)
                if curr_time not in snapshots_list(rbd_image):
                    rbd_image.flush()
                    cmd = 'rbd snap create %s --snap %s' % (rbd_name, curr_time)
                    exec_with_timeout(execute, (cmd,))
                    #exec_with_timeout(rbd_image.create_snap, (curr_time,))
                else:
                    continue
            LOG.info("Thawing %s (+%s sec)" \
                    % (instance_name, timedelta(datetime.now(), time1)))
            thaw_vm(dom)
            LOG.info("Snapshots finished (+%s sec)" \
                    % timedelta(datetime.now(), time1))
        except libvirt.libvirtError as msg:
            LOG.warning("Error occured while freezing/thawing instance: %s" % msg)
            LOG.warning("Falling back to non-quiesced snapshots!")
            for rbd_image in rbd_list:
                sleep(5)
                rbd_name = "%s/%s" % (detect_pool(rbd_image.name), rbd_image.name)
                if curr_time not in snapshots_list(rbd_image):
                    rbd_image.flush()
                    cmd = 'rbd snap create %s --snap %s' % (rbd_name, curr_time)
                    exec_with_timeout(execute, (cmd,))
                else:
                    continue
        except Exception as msg:
            LOG.exception(msg)
    else:
        if not dom.isActive():
            LOG.info("Instance is powered off, quiescing not needed!")
        elif not guest_agent_available(dom):
            LOG.warning("QEMU guest agent not available, quiescing disabled!")
        for rbd_image in rbd_list:
            sleep(5)
            rbd_name = "%s/%s" % (detect_pool(rbd_image.name), rbd_image.name)
            if curr_time not in snapshots_list(rbd_image):
                rbd_image.flush()
                cmd = 'rbd snap create %s --snap %s' % (rbd_name, curr_time)
                exec_with_timeout(execute, (cmd,))
            else:
                continue
    LOG.info("Snapshots finished for %s" % instance_name)

def guest_agent_available(dom):
    cmd = '{"execute":"guest-ping"}'
    try:
        out = qemuAgentCommand(dom, cmd, 30, 0)
    except libvirt.libvirtError:
        return False
    return True

def snapshots_list(rbd_image):
    snap_list = [snap for snap in list(rbd_image.list_snaps()) if looks_like_date(snap['name'])]
    sorted_by_time = [ snap['name'] for snap in sorted(snap_list, \
                        key=lambda f: strptime(f['name'], TIME_FORMAT)) ]
    sorted_by_id   = [ snap['name'] for snap in sorted(snap_list, \
                        key=lambda f: int(f['id']))]
    if sorted_by_time != sorted_by_id:
        LOG.warning("Snapshots list of %s is not ordered correctly, please check!" 
                % rbd_image.name)
    return sorted_by_time

def remove_all_snapshots(rbd_list):
    for rbd_image in rbd_list:
        LOG.info("Removing all snapshots from %s" % rbd_image.name)
        for snap in reversed(snapshots_list(rbd_image)):
            rbd_image.remove_snap(snap)

def is_clone(rbd_image):
    try:    
        parent = rbd_image.parent_info()
        return True if parent else False
    except rbd.ImageNotFound:
        return False

def current_time():
    return strftime(TIME_FORMAT, localtime())

def timedelta(time2, time1):
    return (time2 - time1).total_seconds()

def ensure_dir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)

def backup_folder(instance):
    folder = instance.name.replace(" ", "_").replace("/", "") + "_" + instance.id
    return "/".join((BACKUPS_TOP_DIR, folder))

def remove_empty_subdirs(directory):
    for root, dirs, files in os.walk(directory):
        for dir in dirs:
            dir = os.path.join(root, dir)
            if not os.listdir(dir):
                try:
                    os.rmdir(dir)
                except:
                    pass

def export_diff(instance, rbd_list, full_backup=False):
    res = 0
    dest_dir = None
    curr_time = current_time()
    for rbd_image in rbd_list:
        snaps_list = snapshots_list(rbd_image)
        if not snaps_list:
            LOG.error("No proper snapshots found for image %s ! Backup is not possible!" % rbd_image.name)
            dest_dir = "/".join((backup_folder(instance), curr_time))
            ensure_dir(dest_dir)
            res += 1
            continue
        snap = snaps_list[-1]
        pool = detect_pool(rbd_image.name)
        dest_dir = "/".join((backup_folder(instance), snap))
        ensure_dir(dest_dir)
        if full_backup:
            LOG.info("Export RBD image %s" % rbd_image.name)
            filename = "full_" + rbd_image.name
            cmd = "rbd export --no-progress %s/%s --snap %s " \
                    % (pool, rbd_image.name, snap)
        else:
            if len(snaps_list)==1:
                LOG.error("Only one snapshot found for image %s ! Incremental backup is not possible!" % rbd_image.name)
                dest_dir = "/".join((backup_folder(instance), curr_time))
                ensure_dir(dest_dir)
                res += 1
                continue
            LOG.info("Export-diff RBD image %s" % rbd_image.name)
            filename = "inc_" + rbd_image.name
            from_snap = None
            # Find the latest snapshot for which backup of any type is available
            # In normal conditions this snapshot must be the first one
            # We must start from the second from the end (-2 index)
            for n in range(2, len(snaps_list)+1):
                if backup_is_available(instance, snaps_list[-n], rbd_image.name):
                    from_snap = snaps_list[-n]
                    break
            if not from_snap:
                LOG.error("DANGER! No previous backups found for current RBD snapshots! Backup chain is likely to be broken")
                res+=1
                continue
            cmd = "rbd export-diff --no-progress %s/%s --snap %s --from-snap %s " \
                    % (pool, rbd_image.name, snap, from_snap)
        dest_file = os.path.join(dest_dir, filename)
        cmd += dest_file
        out, rc = execute(cmd)
        if rc==0: 
            if full_backup:
                LOG.info("Full backup of %s successfully finished." % rbd_image.name)
            else:
                LOG.info("Incremental backup of %s successfully finished." % rbd_image.name)
            map(rbd_image.remove_snap, snaps_list[:-1])
            continue
        else:
            res += 1
            LOG.error("RBD export failed: %s" % out)
    if dest_dir:
        status_file = os.path.join(dest_dir, 'status')
        with open(status_file, "w+") as f:
            f.write(str(res) + '\n')
    remove_empty_subdirs(backup_folder(instance))
    if full_backup and res == 0:
        backups = get_backups(instance)
        full_backup_dates = [d for d in sorted(backups.keys()) if backups[d].get('type') == 'full']
        if len(backups) > 7*(BACKUP_RETENTION_WEEKS+1) and len(full_backup_dates)>=2:
            date1, date2 = full_backup_dates[0], full_backup_dates[1]
            upper_dir = os.path.join(dest_dir, '..')
            for root, dirs, files in os.walk(upper_dir):
                for dir in dirs:
                    if date1 <= dir < date2:
                        shutil.rmtree(os.path.join(root, dir))
    return res

def rbd_import(rbd_image, filepath):
    pool = detect_pool(rbd_image)
    cmd = "rbd import  --no-progress %s %s/%s" % (filepath, pool, rbd_image)
    out, rc = execute(cmd)
    if rc!=0:
        #TODO Correct error LOG
        LOG.error(out)

def import_diff(rbd_image, filepath):
    pool = detect_pool(rbd_image)
    cmd = "rbd import-diff --no-progress %s %s/%s" % (filepath, pool, rbd_image)
    out, rc = execute(cmd)
    if rc!=0:
        #TODO Correct error LOG
        LOG.error(out)

def full_backup_available(instance):
    backups = get_backups(instance).values()
    if not backups or all([backup.get('type')!='full' for backup in backups]):
        return False
    else:
        return True

def instance_backup(instance, dom, rbd_list, full_backup=False):
    LOG.info("="*80)
    if full_backup:
        LOG.info("Taking full backup of instance %s" % instance.name)
        remove_all_snapshots(rbd_list)
    else:
        # If no full backup found - take full backup instead of incremental
        if not full_backup_available(instance):
            full_backup = True
            LOG.info("No previous full backup found of %s" % instance.name) 
            LOG.info("Taking full backup of instance %s" % instance.name)
        else:
            LOG.info("Taking incremental backup of instance %s" % instance.name)
    take_rbd_snapshots(dom, rbd_list, instance.name)
    res = export_diff(instance, rbd_list, full_backup=full_backup)
    if res == 0:
        LOG.info("Done")
    LOG.info("="*80)

def restore_instance_inplace(instance, dest_date):
    LOG.info("Performing inplace restore of instance %s to date %s" % (instance.name, dest_date))
    if instance_is_running(instance):
        LOG.info("Powering off instance.", end="")
        instance.stop()
        while instance_is_running(instance):
            print('.', end='')
            sleep(2)
        print("\nDone")
    else:
        LOG.info("Instance is already powered off")
    root_rbd_id = str(instance.id + "_disk")
    volume_ids = [str('volume-' + vol.id) for vol in nova.volumes.get_server_volumes(instance.id)]
    backups = get_backups(instance)
    if dest_date not in backups.keys():
        LOG.error("Invalid restore date was specified")
        return
    for date in backups.copy():
        if date > dest_date:
            backups.pop(date)
    try:
        for date in sorted(backups.keys()):
            if backups[date]['type']=='full':
                for file in backups[date]['files']:
                    if root_rbd_id in file:
                        if rbd_image_exists(root_rbd_id + ".bak"):
                            delete_rbd_by_name(root_rbd_id + ".bak")
                        rename_rbd(root_rbd_id, root_rbd_id + ".bak")
                        rbd_import(root_rbd_id, file)
                        rbd_snap_create(root_rbd_id, str(date))
                    else:
                        for volume in volume_ids:
                            if volume in file:
                                if rbd_image_exists(volume + ".bak"):
                                    delete_rbd_by_name(volume + ".bak")
                                rename_rbd(volume, volume + ".bak")
                                rbd_import(volume, file)
                                rbd_snap_create(volume, str(date))
            elif backups[date]['type']=='inc':
                for file in backups[date]['files']:
                    if root_rbd_id in file:
                        import_diff(root_rbd_id, file)
                    else:
                        for volume in volume_ids:
                            if volume in file:
                                import_diff(volume, file)
    except:
        raise
    finally:
        LOG.info("Starting instance after the restore")
        instance.start()

def get_backups(instance):
    backups = {}
    backup_dir = backup_folder(instance)
    if os.path.exists(backup_dir):
        dates = [dir for dir in os.listdir(backup_dir) if os.path.isdir(os.path.join(backup_dir, dir))]
        for date in dates:
            status_file = os.path.join(backup_dir, date, 'status')
            status = STATUS_ERROR
            if os.path.exists(status_file):
                with open(status_file) as f:
                    status = f.read().strip()
            if status == '0':
                status = STATUS_OK
            os.chdir(os.path.join(backup_dir, date))
            backups[date] = {}
            full_root_backups = map(os.path.realpath, glob('./full_*_disk'))
            inc_root_backups  = map(os.path.realpath, glob('./inc_*_disk'))
            full_vol_backups  = map(os.path.realpath, glob('./full_volume-*'))
            inc_vol_backups   = map(os.path.realpath, glob('./inc_volume-*'))
            if (full_root_backups or full_vol_backups) and not (inc_root_backups or inc_vol_backups):
                backups[date] = { 'type': 'full', 
                                  'files': full_root_backups + full_vol_backups,
                                  'status': status }
            if (inc_root_backups or inc_vol_backups) and not (full_root_backups or full_vol_backups):
                backups[date] = { 'type': 'inc', 
                                  'files': inc_root_backups + inc_vol_backups,
                                  'status': status }
    return backups

def backup_is_available(instance, date, rbd_name):
    backup_dir = "/".join((backup_folder(instance), date))
    if not os.path.isdir(backup_dir) or not os.listdir(backup_dir):
        return False
    if any([rbd_name in file for file in os.listdir(backup_dir)]):
        return True
    else:
        return False

def p(width, date):
    w = '{:%s}' % width
    return w.format(date)

def print_backup_list_header():
    INSTANCE = p(INSTANCE_LEN, 'INSTANCE')
    TENANT = p(TENANT_LEN, 'PROJECT')
    DATE = p(DATE_LEN, 'DATE')
    TYPE = p(TYPE_LEN, 'TYPE')
    SIZE = p(SIZE_LEN, 'SIZE(GB)')
    STATUS = p(STATUS_LEN, 'STATUS')
    print(d.join(("", INSTANCE, TENANT, DATE, TYPE, SIZE, STATUS, "")))

def display_date(date):
    date = date.split('-')
    time = ":".join((date[3], date[4]))
    date = "-".join((date[2], date[1], date[0]))
    return " ".join((date, time))

def format_user_date(date):
    date = date.split(' ')
    time = date[1].replace(':', '-')
    date = '-'.join(date[0].split('-')[::-1])
    return '-'.join((date, time))

def display_backups(instance):
    def print_line(instance, tenant, date, backup_type, size, status):
        print(d.join(("", p(INSTANCE_LEN, instance), p(TENANT_LEN, tenant), p(DATE_LEN, date), \
                p(TYPE_LEN, backup_type), p(SIZE_LEN, str(size)), p(STATUS_LEN+9, status), "")))
    bs = get_backups(instance)
    tenant = get_tenant_name_by_id(instance.tenant_id, instance.id)
    if bs:
        backup_dir = backup_folder(instance)
        for i, b in enumerate(sorted(bs.keys())):
            size = float(0)
            files = bs[b].get('files')
            if not files:
                files = []
            for file in files:
                file_size, rc = execute("du -k %s | cut -f1" % file)
                size += round(float(file_size)/1048576.0, 2)
            status = bs[b].get('status')
            status = GREEN + STATUS_OK + END if status == STATUS_OK \
                    else RED + STATUS_ERROR + END
            backup_type = bs[b].get('type')
            if i == 0 and backup_type:
                print_line(instance.name, tenant, display_date(b), backup_type, size, status)
            elif i == 0 and not backup_type:
                print_line(instance.name, tenant, display_date(b), '---', '---', status)
            elif backup_type:
                print_line("", "", display_date(b), backup_type, size, status)
            elif len(bs)>1 and i!=0:
                print_line("", "", display_date(b), '---', '---', status)
            else:
                print_line(instance.name, tenant, display_date(b), '---', '---', status)
    else:
        l = '{:^%s}' % (DATE_LEN + TYPE_LEN + SIZE_LEN + STATUS_LEN + len(YELLOW) + len(END) + 6)
        if not instance_in_ceph(instance):
            print(d.join(("", p(INSTANCE_LEN, instance.name), p(TENANT_LEN, tenant), \
                    l.format(YELLOW + "-- Nothing to backup --" + END ), "")))
        else:
            print(d.join(("", p(INSTANCE_LEN, instance.name), p(TENANT_LEN, tenant), \
                    l.format(RED + "-- No backups found --" + END ), "")))

def instance_in_ceph(instance):
    if instance.metadata.get('storage')=='rbd:ceph/vms':
        return True
    elif not LIST_BACKUPS:
        LOG.warning("Instance %s is not stored in Ceph, its root disk will not be backed up (volumes will be)!" \
                % instance.name)
        return False

def get_instance_list(instance_list=None, tenant_list=None):
    res = []
    if instance_list:
        for instance_name in instance_list.split(','):
            instance_name = instance_name.strip()
            instance = instances = None
            if looks_like_uuid(instance_name):
                instance = nova.servers.get(instance_name)
                if not instance:
                    LOG.warning("Cannot find instance with ID %s" % instance_name)
                elif instance_name in (instance.name, instance.id):
                    res.append(instance)
            else:
                instances = nova.servers.list(search_opts={ 'name': instance_name, 'all_tenants': True })
                instances = [inst for inst in instances if inst.name == instance_name]
                if not instances:
                    LOG.warning("Cannot find instance with name %s" % instance_name)
                else:
                    res.extend(instances)
    if tenant_list:
        for tenant_id in get_tenant_id_list(tenant_list):
            instances = nova.servers.list(search_opts={ 'tenant_id': tenant_id , 'all_tenants': True })
            if not instances:
                LOG.warning("No instances found in tenant %s" % tenant_id)
            else:
                res.extend(instances)
    return res

def get_volume_list(volume_list=None, tenant_list=None, instance_list=None):
    res = []
    if volume_list:
        for volume_name in volume_list.split(','):
            volume_name = volume_name.strip()
            volume = volumes = None
            if looks_like_uuid(volume_name):
                volume = cinder.volumes.get(volume_name)
                if not volume:
                    LOG.warning("Cannot find volume with ID %s" % volume_name)
                elif volume_name in (volume.name, volume.id):
                    res.append(volume)
            else:
                volumes = cinder.volumes.list( search_opts={'name': volume_name, 'all_tenants': True} )
                volumes = [vol for vol in volumes if vol.name == volume_name]
                if not volumes:
                    LOG.warning("Cannot find volume with name %s" % volume_name)
                else:
                    res.extend(volumes)
    if tenant_list:
        for tenant_id in get_tenant_id_list(tenant_list):
            volumes = cinder.volumes.list( search_opts={'project_id': tenant_id, 'all_tenants': True} )
            if not volumes:
                LOG.warning("No volumes found in tenant %s" % tenant_id)
            else:
                res.extend(volumes)
    # Don't include volumes that are already attached to target instances
    instance_id_list = [instance.id for instance in instance_list]
    res1 = [vol for vol in res if vol.attachments and vol.attachments[0]['server_id'] in instance_id_list]
    res = [vol for vol in res if vol not in res1]
    return res

def get_tenant_id_list(tenant_list):
    tenant_ids = []
    for tenant_name in tenant_list.split(','):
        tenant_name = tenant_name.strip()
        instances = tenant_id = None
        if looks_like_uuid(tenant_name):
            tenant_id = keystone.projects.get(tenant_name).id
        else:
            tenant_id = all_tenants[tenant_name]
        if not tenant_id:
            LOG.warning("Cannot find tenant with name or ID %s" % tenant_name)
        else:
            tenant_ids.append(tenant_id)
    return tenant_ids

def get_tenant_name_by_id(tenant_id, instance_id):
    tenants = [k for k,v in all_tenants.iteritems() if v==tenant_id]
    if tenants:
        return tenants[0]
    else:
        LOG.warning("No tenant with ID %s found for instance ID %s" \
                        % (tenant_id, instance_id))

def remove_duplicates(obj_list):
    obj_ids = []
    res = []
    for obj in obj_list:
        if not obj.id in obj_ids:
            obj_ids.append(obj.id)
            res.append(obj)
    return res

def remove_instance_from_list(inst_list, instance):
    return [inst for inst in inst_list if inst.id!=instance.id]

def get_backup_targets(config):
    res = {}
    res1 = get_instance_list(instance_list=config.get('targets', 'instances'))
    res2 = get_instance_list(tenant_list=config.get('targets', 'tenants'))
    res3 = get_instance_list(instance_list=config.get('targets', 'instances_with_root_disk'))
    res4 = get_instance_list(tenant_list=config.get('targets', 'tenants_with_root_disk'))
    instances = remove_duplicates(res1 + res2 + res3 + res4)
    res5 = get_volume_list(volume_list=config.get('targets', 'volumes'), 
                            tenant_list=config.get('targets', 'tenants_volumes'),
                            instance_list=instances)
    # Instances in "with_root_disk" list but not in Ceph must be anyway backed up like "without_root_disk"
    # Here we transfer such instances to without_root_disks list
    for instance in res3[:]:
        if not instance_in_ceph(instance):
            res1.append(instance)
            res3 = remove_instance_from_list(res3, instance)
    for instance in res4[:]:
        if not instance_in_ceph(instance):
            res1.append(instance)
            res4 = remove_instance_from_list(res4, instance)
    res['with_root_disk'] = remove_duplicates(res3 + res4)
    res['without_root_disk'] = remove_duplicates(res1 + res2)
    res['volumes'] = remove_duplicates(res5)
    return res

def volume_backup(volume, full_backup=True):
    #TODO Not implemented yet!
    rbd_id = str('volume-' + volume.id)
    rbd_image = rbd.Image(VOLUMES_POOL_IOCTX, rbd_id)
    if volume.attachments:
        host = virsh_name = libvirt_conn = dom = None
        server_id = volume.attachments[0]['server_id']
        instance = nova.servers.get(server_id)
        if instance_is_running(instance):
            host = getattr(instance, 'OS-EXT-SRV-ATTR:hypervisor_hostname')
            virsh_name = getattr(instance, 'OS-EXT-SRV-ATTR:instance_name')
            libvirt_conn=libvirt.open(LIBVIRT_URI % host)
            dom = libvirt_conn.lookupByName(virsh_name)
            take_rbd_snapshots(dom, [rbd_image], 'volume %s/%s' % \
                                (detect_pool(rbd_image.name), rbd_image.name))

def its_show_time(config):
    sched_full = config.get('schedule', 'full')
    sched_inc = config.get('schedule', 'incremental')
    week = { 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6, 'sun': 7 }
    full_backup_weekday, full_backup_time = [s.strip().lower() for s in sched_full.split(',')]
    inc_backup_time = sched_inc.strip().lower()
    #if datetime.now() 
    #if week[full_backup_weekday] == datetime.now().isoweekday():

def exception_handler(type, value, tb):
    LOG.exception("".join(traceback.format_exception(type, value, tb)))


default_conf        = '/etc/ceph-backup.conf'
args, config        = parse_args_and_config()
defaults            = dict(config.items("default"))
OS_USERNAME         = defaults['os_username']
OS_PASSWORD         = defaults['os_password']
OS_AUTH_URL         = defaults['os_auth_url']
OS_PROJECT_NAME     = defaults['os_project_name']
USER_DOMAIN_NAME    = defaults['user_domain_name']
PROJECT_DOMAIN_NAME = defaults['project_domain_name']
VMS_POOL            = defaults['vms_pool']
VOLUMES_POOL        = defaults['volumes_pool']
BACKUPS_TOP_DIR     = defaults['backups_top_dir']
LIBVIRT_URI         = defaults['libvirt_uri']
SSH_USER            = defaults['ssh_user']
SSH_KEY             = defaults['ssh_key']
LOG_FILE            = defaults['log_file']
BACKUP_RETENTION_WEEKS = int(defaults['backup_retention_weeks'])
ceph_cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
ceph_cluster.connect()
VMS_POOL_IOCTX = ceph_cluster.open_ioctx(VMS_POOL)
VOLUMES_POOL_IOCTX = ceph_cluster.open_ioctx(VOLUMES_POOL)


# Get Keystone, Nova and Cinder sessions
session, keystone = get_keystone_session()
nova = novaclient.Client(2, session=session)
cinder = cinderclient.Client(session=session)
all_tenants = dict([(tenant.name, tenant.id) for tenant in keystone.projects.list()])

# Logging settings
LOG.basicConfig(filename=LOG_FILE, level=LOG.INFO,
                format="%(asctime)s %(levelname)s: %(message)s", 
                datefmt="%Y-%m-%d %H:%M:%S")
# Skip annoying info spam from "requests" and "paramiko"
LOG.getLogger("requests").setLevel(LOG.WARNING)
LOG.getLogger("paramiko").setLevel(LOG.WARNING)
# Redirect uncaught exceptions to log file
if not sys.stdout.isatty():
    sys.excepthook = exception_handler
# Print logs to stdout also if it's a user terminal
elif not args.list_backups:
    log_to_stdout = LOG.StreamHandler()
    fmt = LOG.Formatter('%(levelname)s: %(message)s')
    log_to_stdout.setFormatter(fmt)
    LOG.getLogger().addHandler(log_to_stdout)

check_directory_is_writeable(BACKUPS_TOP_DIR)
BACKUP_TYPE = args.backup_type
RESTORE_DATE = args.restore_date
LIST_BACKUPS = args.list_backups

if args.instances:
    INSTANCE_LIST = get_instance_list(instance_list=",".join(args.instances))
    INSTANCES_WITH_ROOT = []
    INSTANCES_WITHOUT_ROOT = []
else:
    BACKUP_TARGETS = get_backup_targets(config)
    INSTANCES_WITH_ROOT = BACKUP_TARGETS['with_root_disk']
    INSTANCES_WITHOUT_ROOT = BACKUP_TARGETS['without_root_disk']
    VOLUMES_LIST = BACKUP_TARGETS['volumes']
    INSTANCE_LIST = INSTANCES_WITH_ROOT + INSTANCES_WITHOUT_ROOT

if args.instances and not LIST_BACKUPS and not BACKUP_TYPE and not RESTORE_DATE:
    print("ERROR: Instance list given but no action specified (choose from -b, -r or -l)")
if LIST_BACKUPS:
    print(header.replace("-","="))
    print_backup_list_header()
    print(header.replace("-","="))
for instance in sorted(INSTANCE_LIST, key=lambda f: f.tenant_id):
    if LIST_BACKUPS:
        display_backups(instance)
        print(header)
        continue
    elif (BACKUP_TYPE and INSTANCE_LIST) or (instance in INSTANCES_WITH_ROOT) or \
            (instance in INSTANCES_WITHOUT_ROOT):
        host = virsh_name = libvirt_conn = dom = None
        rbd_list = []
        # Check instance was launched from image, otherwise skip Nova RBD disks lookup
        if instance.image and ((args.backup_root_disks and instance_in_ceph(instance)) or \
                instance in INSTANCES_WITH_ROOT):
            rbd_id = str(instance.id + "_disk")
            rbd_list.append(rbd.Image(VMS_POOL_IOCTX, rbd_id))
        volumes_attached = nova.volumes.get_server_volumes(instance.id)
        host = getattr(instance, 'OS-EXT-SRV-ATTR:hypervisor_hostname')
        virsh_name = getattr(instance, 'OS-EXT-SRV-ATTR:instance_name')
        libvirt_conn=libvirt.open(LIBVIRT_URI % host)
        dom = libvirt_conn.lookupByName(virsh_name)
        if volumes_attached:
            for volume in volumes_attached:
                vol_id = str('volume-' + volume.id)
                rbd_list.append(rbd.Image(VOLUMES_POOL_IOCTX, vol_id))
        if not rbd_list:
            # Instances with root disks not chosen for backup and having no
            # volumes attached or with root disk not in Ceph have nothing to backup
            LOG.warning("Nothing to backup for instance %s" % instance.name)
            continue
        #for rbd_img in rbd_list:
        #    if is_clone(rbd_img):
        #        LOG.info("Flattening RBD image %s/%s" % \
        #                (detect_pool(rbd_img.name), rbd_img.name))
        #        rbd_img.flatten()
        if BACKUP_TYPE=='full':
            instance_backup(instance, dom, rbd_list, full_backup=True)
        elif BACKUP_TYPE=='inc':
            instance_backup(instance, dom, rbd_list)
        for rbd_image in rbd_list:
            rbd_image.close()
        libvirt_conn.close()
        continue
    elif RESTORE_DATE:
        if looks_like_date(RESTORE_DATE):
            pass
        elif looks_like_date(RESTORE_DATE, time_format=USER_TIME_FORMAT):
            RESTORE_DATE = format_user_date(RESTORE_DATE)
        else:
            raise Exception("Restore date doesn't match date format: %s or %s" 
                                % (TIME_FORMAT, USER_TIME_FORMAT))
            sys.exit(1)
        if len(INSTANCE_LIST)>1:
            print("ERROR: You may specify only a single instance to restore")
            sys.exit(1)
        restore_instance_inplace(instance, RESTORE_DATE)
    elif RESTORE_DATE and not looks_like_date(RESTORE_DATE):
        raise Exception("Restore date doesn't match date format: %s" % TIME_FORMAT)
#    if VOLUMES_LIST:
#        for volume in VOLUMES_LIST:
#               full_backup(instance, dom, [rbd_image])
#                incremental_backup(instance, dom, [rbd_image])
#            else:
#                rbd_snap_create(rbd_name, snap_name)

## Clean up old files and sessions 
rbd_inst = rbd.RBD()
old_images = [image for image in rbd_inst.list(VMS_POOL_IOCTX) + \
                rbd_inst.list(VOLUMES_POOL_IOCTX) if image.endswith('.bak')]
for image in old_images:
    with get_rbd_image_obj(image) as rbd_img:
        remove_all_snapshots([rbd_img])
    try:
        rbd_inst.remove(get_pool_ioctx(image), image)
    except rbd.ImageNotFound:
        pass
VMS_POOL_IOCTX.close()
VOLUMES_POOL_IOCTX.close()
ceph_cluster.shutdown()
