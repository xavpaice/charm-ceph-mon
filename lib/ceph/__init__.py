# Copyright 2016 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import ctypes
import collections
import json
import random
import socket
import subprocess
import time
import os
import re
import sys
import errno
import shutil
import pyudev

from datetime import datetime

from charmhelpers.core import hookenv
from charmhelpers.core import templating
from charmhelpers.core.host import (
    chownr,
    cmp_pkgrevno,
    lsb_release,
    mkdir,
    mounts,
    owner,
    service_restart,
    service_start,
    service_stop,
    CompareHostReleases,
)
from charmhelpers.core.hookenv import (
    cached,
    config,
    log,
    status_set,
    DEBUG,
    ERROR,
    WARNING,
)
from charmhelpers.fetch import (
    apt_cache,
    add_source, apt_install, apt_update)
from charmhelpers.contrib.storage.linux.ceph import (
    monitor_key_set,
    monitor_key_exists,
    monitor_key_get,
    get_mon_map,
)
from charmhelpers.contrib.storage.linux.utils import (
    is_block_device,
    zap_disk,
    is_device_mounted,
)
from charmhelpers.contrib.openstack.utils import (
    get_os_codename_install_source,
)

from ceph.ceph_helpers import check_output

CEPH_BASE_DIR = os.path.join(os.sep, 'var', 'lib', 'ceph')
OSD_BASE_DIR = os.path.join(CEPH_BASE_DIR, 'osd')
HDPARM_FILE = os.path.join(os.sep, 'etc', 'hdparm.conf')

LEADER = 'leader'
PEON = 'peon'
QUORUM = [LEADER, PEON]

PACKAGES = ['ceph', 'gdisk', 'ntp', 'btrfs-tools', 'python-ceph',
            'radosgw', 'xfsprogs', 'python-pyudev']

LinkSpeed = {
    "BASE_10": 10,
    "BASE_100": 100,
    "BASE_1000": 1000,
    "GBASE_10": 10000,
    "GBASE_40": 40000,
    "GBASE_100": 100000,
    "UNKNOWN": None
}

# Mapping of adapter speed to sysctl settings
NETWORK_ADAPTER_SYSCTLS = {
    # 10Gb
    LinkSpeed["GBASE_10"]: {
        'net.core.rmem_default': 524287,
        'net.core.wmem_default': 524287,
        'net.core.rmem_max': 524287,
        'net.core.wmem_max': 524287,
        'net.core.optmem_max': 524287,
        'net.core.netdev_max_backlog': 300000,
        'net.ipv4.tcp_rmem': '10000000 10000000 10000000',
        'net.ipv4.tcp_wmem': '10000000 10000000 10000000',
        'net.ipv4.tcp_mem': '10000000 10000000 10000000'
    },
    # Mellanox 10/40Gb
    LinkSpeed["GBASE_40"]: {
        'net.ipv4.tcp_timestamps': 0,
        'net.ipv4.tcp_sack': 1,
        'net.core.netdev_max_backlog': 250000,
        'net.core.rmem_max': 4194304,
        'net.core.wmem_max': 4194304,
        'net.core.rmem_default': 4194304,
        'net.core.wmem_default': 4194304,
        'net.core.optmem_max': 4194304,
        'net.ipv4.tcp_rmem': '4096 87380 4194304',
        'net.ipv4.tcp_wmem': '4096 65536 4194304',
        'net.ipv4.tcp_low_latency': 1,
        'net.ipv4.tcp_adv_win_scale': 1
    }
}


class Partition(object):
    def __init__(self, name, number, size, start, end, sectors, uuid):
        """
        A block device partition
        :param name: Name of block device
        :param number:  Partition number
        :param size:  Capacity of the device
        :param start:  Starting block
        :param end:  Ending block
        :param sectors:  Number of blocks
        :param uuid:  UUID of the partition
        """
        self.name = name,
        self.number = number
        self.size = size
        self.start = start
        self.end = end
        self.sectors = sectors
        self.uuid = uuid

    def __str__(self):
        return "number: {} start: {} end: {} sectors: {} size: {} " \
               "name: {} uuid: {}".format(self.number, self.start,
                                          self.end,
                                          self.sectors, self.size,
                                          self.name, self.uuid)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


def unmounted_disks():
    """List of unmounted block devices on the current host."""
    disks = []
    context = pyudev.Context()
    for device in context.list_devices(DEVTYPE='disk'):
        if device['SUBSYSTEM'] == 'block':
            matched = False
            for block_type in [u'dm', u'loop', u'ram', u'nbd']:
                if block_type in device.device_node:
                    matched = True
            if matched:
                continue
            disks.append(device.device_node)
    log("Found disks: {}".format(disks))
    return [disk for disk in disks if not is_device_mounted(disk)]


def save_sysctls(sysctl_dict, save_location):
    """
    Persist the sysctls to the hard drive.
    :param sysctl_dict: dict
    :param save_location: path to save the settings to
    :raise: IOError if anything goes wrong with writing.
    """
    try:
        # Persist the settings for reboots
        with open(save_location, "w") as fd:
            for key, value in sysctl_dict.items():
                fd.write("{}={}\n".format(key, value))

    except IOError as e:
        log("Unable to persist sysctl settings to {}.  Error {}".format(
            save_location, e.message), level=ERROR)
        raise


def tune_nic(network_interface):
    """
    This will set optimal sysctls for the particular network adapter.
    :param network_interface: string The network adapter name.
    """
    speed = get_link_speed(network_interface)
    if speed in NETWORK_ADAPTER_SYSCTLS:
        status_set('maintenance', 'Tuning device {}'.format(
            network_interface))
        sysctl_file = os.path.join(
            os.sep,
            'etc',
            'sysctl.d',
            '51-ceph-osd-charm-{}.conf'.format(network_interface))
        try:
            log("Saving sysctl_file: {} values: {}".format(
                sysctl_file, NETWORK_ADAPTER_SYSCTLS[speed]),
                level=DEBUG)
            save_sysctls(sysctl_dict=NETWORK_ADAPTER_SYSCTLS[speed],
                         save_location=sysctl_file)
        except IOError as e:
            log("Write to /etc/sysctl.d/51-ceph-osd-charm-{} "
                "failed. {}".format(network_interface, e.message),
                level=ERROR)

        try:
            # Apply the settings
            log("Applying sysctl settings", level=DEBUG)
            check_output(["sysctl", "-p", sysctl_file])
        except subprocess.CalledProcessError as err:
            log('sysctl -p {} failed with error {}'.format(sysctl_file,
                                                           err.output),
                level=ERROR)
    else:
        log("No settings found for network adapter: {}".format(
            network_interface), level=DEBUG)


def get_link_speed(network_interface):
    """
    This will find the link speed for a given network device.  Returns None
    if an error occurs.
    :param network_interface: string The network adapter interface.
    :return: LinkSpeed
    """
    speed_path = os.path.join(os.sep, 'sys', 'class', 'net',
                              network_interface, 'speed')
    # I'm not sure where else we'd check if this doesn't exist
    if not os.path.exists(speed_path):
        return LinkSpeed["UNKNOWN"]

    try:
        with open(speed_path, 'r') as sysfs:
            nic_speed = sysfs.readlines()

            # Did we actually read anything?
            if not nic_speed:
                return LinkSpeed["UNKNOWN"]

            # Try to find a sysctl match for this particular speed
            for name, speed in LinkSpeed.items():
                if speed == int(nic_speed[0].strip()):
                    return speed
            # Default to UNKNOWN if we can't find a match
            return LinkSpeed["UNKNOWN"]
    except IOError as e:
        log("Unable to open {path} because of error: {error}".format(
            path=speed_path,
            error=e.message), level='error')
        return LinkSpeed["UNKNOWN"]


def persist_settings(settings_dict):
    # Write all settings to /etc/hdparm.conf
    """
        This will persist the hard drive settings to the /etc/hdparm.conf file
        The settings_dict should be in the form of {"uuid": {"key":"value"}}
        :param settings_dict: dict of settings to save
    """
    if not settings_dict:
        return

    try:
        templating.render(source='hdparm.conf', target=HDPARM_FILE,
                          context=settings_dict)
    except IOError as err:
        log("Unable to open {path} because of error: {error}".format(
            path=HDPARM_FILE, error=err.message), level=ERROR)
    except Exception as e:
        # The templating.render can raise a jinja2 exception if the
        # template is not found. Rather than polluting the import
        # space of this charm, simply catch Exception
        log('Unable to render {path} due to error: {error}'.format(
            path=HDPARM_FILE, error=e.message), level=ERROR)


def set_max_sectors_kb(dev_name, max_sectors_size):
    """
    This function sets the max_sectors_kb size of a given block device.
    :param dev_name: Name of the block device to query
    :param max_sectors_size: int of the max_sectors_size to save
    """
    max_sectors_kb_path = os.path.join('sys', 'block', dev_name, 'queue',
                                       'max_sectors_kb')
    try:
        with open(max_sectors_kb_path, 'w') as f:
            f.write(max_sectors_size)
    except IOError as e:
        log('Failed to write max_sectors_kb to {}. Error: {}'.format(
            max_sectors_kb_path, e.message), level=ERROR)


def get_max_sectors_kb(dev_name):
    """
    This function gets the max_sectors_kb size of a given block device.
    :param dev_name: Name of the block device to query
    :return: int which is either the max_sectors_kb or 0 on error.
    """
    max_sectors_kb_path = os.path.join('sys', 'block', dev_name, 'queue',
                                       'max_sectors_kb')

    # Read in what Linux has set by default
    if os.path.exists(max_sectors_kb_path):
        try:
            with open(max_sectors_kb_path, 'r') as f:
                max_sectors_kb = f.read().strip()
                return int(max_sectors_kb)
        except IOError as e:
            log('Failed to read max_sectors_kb to {}. Error: {}'.format(
                max_sectors_kb_path, e.message), level=ERROR)
            # Bail.
            return 0
    return 0


def get_max_hw_sectors_kb(dev_name):
    """
    This function gets the max_hw_sectors_kb for a given block device.
    :param dev_name: Name of the block device to query
    :return: int which is either the max_hw_sectors_kb or 0 on error.
    """
    max_hw_sectors_kb_path = os.path.join('sys', 'block', dev_name, 'queue',
                                          'max_hw_sectors_kb')
    # Read in what the hardware supports
    if os.path.exists(max_hw_sectors_kb_path):
        try:
            with open(max_hw_sectors_kb_path, 'r') as f:
                max_hw_sectors_kb = f.read().strip()
                return int(max_hw_sectors_kb)
        except IOError as e:
            log('Failed to read max_hw_sectors_kb to {}. Error: {}'.format(
                max_hw_sectors_kb_path, e.message), level=ERROR)
            return 0
    return 0


def set_hdd_read_ahead(dev_name, read_ahead_sectors=256):
    """
    This function sets the hard drive read ahead.
    :param dev_name: Name of the block device to set read ahead on.
    :param read_ahead_sectors: int How many sectors to read ahead.
    """
    try:
        # Set the read ahead sectors to 256
        log('Setting read ahead to {} for device {}'.format(
            read_ahead_sectors,
            dev_name))
        check_output(['hdparm',
                      '-a{}'.format(read_ahead_sectors),
                      dev_name])
    except subprocess.CalledProcessError as e:
        log('hdparm failed with error: {}'.format(e.output),
            level=ERROR)


def get_block_uuid(block_dev):
    """
    This queries blkid to get the uuid for a block device.
    :param block_dev: Name of the block device to query.
    :return: The UUID of the device or None on Error.
    """
    try:
        block_info = check_output(
            ['blkid', '-o', 'export', block_dev])
        for tag in block_info.split('\n'):
            parts = tag.split('=')
            if parts[0] == 'UUID':
                return parts[1]
        return None
    except subprocess.CalledProcessError as err:
        log('get_block_uuid failed with error: {}'.format(err.output),
            level=ERROR)
        return None


def check_max_sectors(save_settings_dict,
                      block_dev,
                      uuid):
    """
    Tune the max_hw_sectors if needed.
    make sure that /sys/.../max_sectors_kb matches max_hw_sectors_kb or at
    least 1MB for spinning disks
    If the box has a RAID card with cache this could go much bigger.
    :param save_settings_dict: The dict used to persist settings
    :param block_dev: A block device name: Example: /dev/sda
    :param uuid: The uuid of the block device
    """
    dev_name = None
    path_parts = os.path.split(block_dev)
    if len(path_parts) == 2:
        dev_name = path_parts[1]
    else:
        log('Unable to determine the block device name from path: {}'.format(
            block_dev))
        # Play it safe and bail
        return
    max_sectors_kb = get_max_sectors_kb(dev_name=dev_name)
    max_hw_sectors_kb = get_max_hw_sectors_kb(dev_name=dev_name)

    if max_sectors_kb < max_hw_sectors_kb:
        # OK we have a situation where the hardware supports more than Linux is
        # currently requesting
        config_max_sectors_kb = hookenv.config('max-sectors-kb')
        if config_max_sectors_kb < max_hw_sectors_kb:
            # Set the max_sectors_kb to the config.yaml value if it is less
            # than the max_hw_sectors_kb
            log('Setting max_sectors_kb for device {} to {}'.format(
                dev_name, config_max_sectors_kb))
            save_settings_dict[
                "drive_settings"][uuid][
                "read_ahead_sect"] = config_max_sectors_kb
            set_max_sectors_kb(dev_name=dev_name,
                               max_sectors_size=config_max_sectors_kb)
        else:
            # Set to the max_hw_sectors_kb
            log('Setting max_sectors_kb for device {} to {}'.format(
                dev_name, max_hw_sectors_kb))
            save_settings_dict[
                "drive_settings"][uuid]['read_ahead_sect'] = max_hw_sectors_kb
            set_max_sectors_kb(dev_name=dev_name,
                               max_sectors_size=max_hw_sectors_kb)
    else:
        log('max_sectors_kb match max_hw_sectors_kb.  No change needed for '
            'device: {}'.format(block_dev))


def tune_dev(block_dev):
    """
    Try to make some intelligent decisions with HDD tuning.  Future work will
    include optimizing SSDs.
    This function will change the read ahead sectors and the max write
    sectors for each block device.
    :param block_dev: A block device name: Example: /dev/sda
    """
    uuid = get_block_uuid(block_dev)
    if uuid is None:
        log('block device {} uuid is None.  Unable to save to '
            'hdparm.conf'.format(block_dev), level=DEBUG)
        return
    save_settings_dict = {}
    log('Tuning device {}'.format(block_dev))
    status_set('maintenance', 'Tuning device {}'.format(block_dev))
    set_hdd_read_ahead(block_dev)
    save_settings_dict["drive_settings"] = {}
    save_settings_dict["drive_settings"][uuid] = {}
    save_settings_dict["drive_settings"][uuid]['read_ahead_sect'] = 256

    check_max_sectors(block_dev=block_dev,
                      save_settings_dict=save_settings_dict,
                      uuid=uuid)

    persist_settings(settings_dict=save_settings_dict)
    status_set('maintenance', 'Finished tuning device {}'.format(block_dev))


def ceph_user():
    if get_version() > 1:
        return 'ceph'
    else:
        return "root"


class CrushLocation(object):
    def __init__(self,
                 name,
                 identifier,
                 host,
                 rack,
                 row,
                 datacenter,
                 chassis,
                 root):
        self.name = name
        self.identifier = identifier
        self.host = host
        self.rack = rack
        self.row = row
        self.datacenter = datacenter
        self.chassis = chassis
        self.root = root

    def __str__(self):
        return "name: {} id: {} host: {} rack: {} row: {} datacenter: {} " \
               "chassis :{} root: {}".format(self.name, self.identifier,
                                             self.host, self.rack, self.row,
                                             self.datacenter, self.chassis,
                                             self.root)

    def __eq__(self, other):
        return not self.name < other.name and not other.name < self.name

    def __ne__(self, other):
        return self.name < other.name or other.name < self.name

    def __gt__(self, other):
        return self.name > other.name

    def __ge__(self, other):
        return not self.name < other.name

    def __le__(self, other):
        return self.name < other.name


def get_osd_weight(osd_id):
    """
    Returns the weight of the specified OSD
    :return: Float :raise: ValueError if the monmap fails to parse.
      Also raises CalledProcessError if our ceph command fails
    """
    try:
        tree = check_output(
            ['ceph', 'osd', 'tree', '--format=json'])
        try:
            json_tree = json.loads(tree)
            # Make sure children are present in the json
            if not json_tree['nodes']:
                return None
            for device in json_tree['nodes']:
                if device['type'] == 'osd' and device['name'] == osd_id:
                    return device['crush_weight']
        except ValueError as v:
            log("Unable to parse ceph tree json: {}. Error: {}".format(
                tree, v.message))
            raise
    except subprocess.CalledProcessError as e:
        log("ceph osd tree command failed with message: {}".format(
            e.message))
        raise


def get_osd_tree(service):
    """
    Returns the current osd map in JSON.
    :return: List. :raise: ValueError if the monmap fails to parse.
      Also raises CalledProcessError if our ceph command fails
    """
    try:
        tree = check_output(
            ['ceph', '--id', service,
             'osd', 'tree', '--format=json'])
        try:
            json_tree = json.loads(tree)
            crush_list = []
            # Make sure children are present in the json
            if not json_tree['nodes']:
                return None
            child_ids = json_tree['nodes'][0]['children']
            for child in json_tree['nodes']:
                if child['id'] in child_ids:
                    crush_list.append(
                        CrushLocation(
                            name=child.get('name'),
                            identifier=child['id'],
                            host=child.get('host'),
                            rack=child.get('rack'),
                            row=child.get('row'),
                            datacenter=child.get('datacenter'),
                            chassis=child.get('chassis'),
                            root=child.get('root')
                        )
                    )
            return crush_list
        except ValueError as v:
            log("Unable to parse ceph tree json: {}. Error: {}".format(
                tree, v.message))
            raise
    except subprocess.CalledProcessError as e:
        log("ceph osd tree command failed with message: {}".format(
            e.message))
        raise


def _get_child_dirs(path):
    """Returns a list of directory names in the specified path.

    :param path: a full path listing of the parent directory to return child
                 directory names
    :return: list. A list of child directories under the parent directory
    :raises: ValueError if the specified path does not exist or is not a
             directory,
             OSError if an error occurs reading the directory listing
    """
    if not os.path.exists(path):
        raise ValueError('Specfied path "%s" does not exist' % path)
    if not os.path.isdir(path):
        raise ValueError('Specified path "%s" is not a directory' % path)

    files_in_dir = [os.path.join(path, f) for f in os.listdir(path)]
    return list(filter(os.path.isdir, files_in_dir))


def _get_osd_num_from_dirname(dirname):
    """Parses the dirname and returns the OSD id.

    Parses a string in the form of 'ceph-{osd#}' and returns the osd number
    from the directory name.

    :param dirname: the directory name to return the OSD number from
    :return int: the osd number the directory name corresponds to
    :raises ValueError: if the osd number cannot be parsed from the provided
                        directory name.
    """
    match = re.search('ceph-(?P<osd_id>\d+)', dirname)
    if not match:
        raise ValueError("dirname not in correct format: %s" % dirname)

    return match.group('osd_id')


def get_local_osd_ids():
    """
    This will list the /var/lib/ceph/osd/* directories and try
    to split the ID off of the directory name and return it in
    a list

    :return: list.  A list of osd identifiers :raise: OSError if
     something goes wrong with listing the directory.
    """
    osd_ids = []
    osd_path = os.path.join(os.sep, 'var', 'lib', 'ceph', 'osd')
    if os.path.exists(osd_path):
        try:
            dirs = os.listdir(osd_path)
            for osd_dir in dirs:
                osd_id = osd_dir.split('-')[1]
                if _is_int(osd_id):
                    osd_ids.append(osd_id)
        except OSError:
            raise
    return osd_ids


def get_local_mon_ids():
    """
    This will list the /var/lib/ceph/mon/* directories and try
    to split the ID off of the directory name and return it in
    a list

    :return: list.  A list of monitor identifiers :raise: OSError if
     something goes wrong with listing the directory.
    """
    mon_ids = []
    mon_path = os.path.join(os.sep, 'var', 'lib', 'ceph', 'mon')
    if os.path.exists(mon_path):
        try:
            dirs = os.listdir(mon_path)
            for mon_dir in dirs:
                # Basically this takes everything after ceph- as the monitor ID
                match = re.search('ceph-(?P<mon_id>.*)', mon_dir)
                if match:
                    mon_ids.append(match.group('mon_id'))
        except OSError:
            raise
    return mon_ids


def _is_int(v):
    """Return True if the object v can be turned into an integer."""
    try:
        int(v)
        return True
    except ValueError:
        return False


def get_version():
    """Derive Ceph release from an installed package."""
    import apt_pkg as apt

    cache = apt_cache()
    package = "ceph"
    try:
        pkg = cache[package]
    except:
        # the package is unknown to the current apt cache.
        e = 'Could not determine version of package with no installation ' \
            'candidate: %s' % package
        error_out(e)

    if not pkg.current_ver:
        # package is known, but no version is currently installed.
        e = 'Could not determine version of uninstalled package: %s' % package
        error_out(e)

    vers = apt.upstream_version(pkg.current_ver.ver_str)

    # x.y match only for 20XX.X
    # and ignore patch level for other packages
    match = re.match('^(\d+)\.(\d+)', vers)

    if match:
        vers = match.group(0)
    return float(vers)


def error_out(msg):
    log("FATAL ERROR: %s" % msg,
        level=ERROR)
    sys.exit(1)


def is_quorum():
    asok = "/var/run/ceph/ceph-mon.{}.asok".format(socket.gethostname())
    cmd = [
        "sudo",
        "-u",
        ceph_user(),
        "ceph",
        "--admin-daemon",
        asok,
        "mon_status"
    ]
    if os.path.exists(asok):
        try:
            result = json.loads(check_output(cmd))
        except subprocess.CalledProcessError:
            return False
        except ValueError:
            # Non JSON response from mon_status
            return False
        if result['state'] in QUORUM:
            return True
        else:
            return False
    else:
        return False


def is_leader():
    asok = "/var/run/ceph/ceph-mon.{}.asok".format(socket.gethostname())
    cmd = [
        "sudo",
        "-u",
        ceph_user(),
        "ceph",
        "--admin-daemon",
        asok,
        "mon_status"
    ]
    if os.path.exists(asok):
        try:
            result = json.loads(check_output(cmd))
        except subprocess.CalledProcessError:
            return False
        except ValueError:
            # Non JSON response from mon_status
            return False
        if result['state'] == LEADER:
            return True
        else:
            return False
    else:
        return False


def wait_for_quorum():
    while not is_quorum():
        log("Waiting for quorum to be reached")
        time.sleep(3)


def add_bootstrap_hint(peer):
    asok = "/var/run/ceph/ceph-mon.{}.asok".format(socket.gethostname())
    cmd = [
        "sudo",
        "-u",
        ceph_user(),
        "ceph",
        "--admin-daemon",
        asok,
        "add_bootstrap_peer_hint",
        peer
    ]
    if os.path.exists(asok):
        # Ignore any errors for this call
        subprocess.call(cmd)


DISK_FORMATS = [
    'xfs',
    'ext4',
    'btrfs'
]

CEPH_PARTITIONS = [
    '89C57F98-2FE5-4DC0-89C1-5EC00CEFF2BE',  # ceph encrypted disk in creation
    '45B0969E-9B03-4F30-B4C6-5EC00CEFF106',  # ceph encrypted journal
    '4FBD7E29-9D25-41B8-AFD0-5EC00CEFF05D',  # ceph encrypted osd data
    '4FBD7E29-9D25-41B8-AFD0-062C0CEFF05D',  # ceph osd data
    '45B0969E-9B03-4F30-B4C6-B4B80CEFF106',  # ceph osd journal
    '89C57F98-2FE5-4DC0-89C1-F3AD0CEFF2BE',  # ceph disk in creation
]


def umount(mount_point):
    """
    This function unmounts a mounted directory forcibly.  This will
    be used for unmounting broken hard drive mounts which may hang.
    If umount returns EBUSY this will lazy unmount.
    :param mount_point: str.  A String representing the filesystem mount point
    :return: int.  Returns 0 on success.  errno otherwise.
    """
    libc_path = ctypes.util.find_library("c")
    libc = ctypes.CDLL(libc_path, use_errno=True)

    # First try to umount with MNT_FORCE
    ret = libc.umount(mount_point, 1)
    if ret < 0:
        err = ctypes.get_errno()
        if err == errno.EBUSY:
            # Detach from try.  IE lazy umount
            ret = libc.umount(mount_point, 2)
            if ret < 0:
                err = ctypes.get_errno()
                return err
            return 0
        else:
            return err
    return 0


def replace_osd(dead_osd_number,
                dead_osd_device,
                new_osd_device,
                osd_format,
                osd_journal,
                reformat_osd=False,
                ignore_errors=False):
    """
    This function will automate the replacement of a failed osd disk as much
    as possible. It will revoke the keys for the old osd, remove it from the
    crush map and then add a new osd into the cluster.
    :param dead_osd_number: The osd number found in ceph osd tree. Example: 99
    :param dead_osd_device: The physical device.  Example: /dev/sda
    :param osd_format:
    :param osd_journal:
    :param reformat_osd:
    :param ignore_errors:
    """
    host_mounts = mounts()
    mount_point = None
    for mount in host_mounts:
        if mount[1] == dead_osd_device:
            mount_point = mount[0]
    # need to convert dev to osd number
    # also need to get the mounted drive so we can tell the admin to
    # replace it
    try:
        # Drop this osd out of the cluster. This will begin a
        # rebalance operation
        status_set('maintenance', 'Removing osd {}'.format(dead_osd_number))
        check_output([
            'ceph',
            '--id',
            'osd-upgrade',
            'osd', 'out',
            'osd.{}'.format(dead_osd_number)])

        # Kill the osd process if it's not already dead
        if systemd():
            service_stop('ceph-osd@{}'.format(dead_osd_number))
        else:
            check_output(['stop', 'ceph-osd', 'id={}'.format(
                dead_osd_number)])
        # umount if still mounted
        ret = umount(mount_point)
        if ret < 0:
            raise RuntimeError('umount {} failed with error: {}'.format(
                mount_point, os.strerror(ret)))
        # Clean up the old mount point
        shutil.rmtree(mount_point)
        check_output([
            'ceph',
            '--id',
            'osd-upgrade',
            'osd', 'crush', 'remove',
            'osd.{}'.format(dead_osd_number)])
        # Revoke the OSDs access keys
        check_output([
            'ceph',
            '--id',
            'osd-upgrade',
            'auth', 'del',
            'osd.{}'.format(dead_osd_number)])
        check_output([
            'ceph',
            '--id',
            'osd-upgrade',
            'osd', 'rm',
            'osd.{}'.format(dead_osd_number)])
        status_set('maintenance', 'Setting up replacement osd {}'.format(
            new_osd_device))
        osdize(new_osd_device,
               osd_format,
               osd_journal,
               reformat_osd,
               ignore_errors)
    except subprocess.CalledProcessError as e:
        log('replace_osd failed with error: ' + e.output)


def get_partition_list(dev):
    """
    Lists the partitions of a block device
    :param dev: Path to a block device. ex: /dev/sda
    :return: :raise:  Returns a list of Partition objects.
        Raises CalledProcessException if lsblk fails
    """
    partitions_list = []
    try:
        partitions = get_partitions(dev)
        # For each line of output
        for partition in partitions:
            parts = partition.split()
            partitions_list.append(
                Partition(number=parts[0],
                          start=parts[1],
                          end=parts[2],
                          sectors=parts[3],
                          size=parts[4],
                          name=parts[5],
                          uuid=parts[6])
            )
        return partitions_list
    except subprocess.CalledProcessError:
        raise


def is_osd_disk(dev):
    partitions = get_partition_list(dev)
    for partition in partitions:
        try:
            info = check_output(['sgdisk', '-i', partition.number, dev])
            info = info.split("\n")  # IGNORE:E1103
            for line in info:
                for ptype in CEPH_PARTITIONS:
                    sig = 'Partition GUID code: {}'.format(ptype)
                    if line.startswith(sig):
                        return True
        except subprocess.CalledProcessError as e:
            log("sgdisk inspection of partition {} on {} failed with "
                "error: {}. Skipping".format(partition.minor, dev, e.message),
                level=ERROR)
    return False


def start_osds(devices):
    # Scan for ceph block devices
    rescan_osd_devices()
    if cmp_pkgrevno('ceph', "0.56.6") >= 0:
        # Use ceph-disk activate for directory based OSD's
        for dev_or_path in devices:
            if os.path.exists(dev_or_path) and os.path.isdir(dev_or_path):
                subprocess.check_call(['ceph-disk', 'activate', dev_or_path])


def rescan_osd_devices():
    cmd = [
        'udevadm', 'trigger',
        '--subsystem-match=block', '--action=add'
    ]

    subprocess.call(cmd)


_bootstrap_keyring = "/var/lib/ceph/bootstrap-osd/ceph.keyring"
_upgrade_keyring = "/var/lib/ceph/osd/ceph.client.osd-upgrade.keyring"


def is_bootstrapped():
    return os.path.exists(_bootstrap_keyring)


def wait_for_bootstrap():
    while not is_bootstrapped():
        time.sleep(3)


def import_osd_bootstrap_key(key):
    if not os.path.exists(_bootstrap_keyring):
        cmd = [
            "sudo",
            "-u",
            ceph_user(),
            'ceph-authtool',
            _bootstrap_keyring,
            '--create-keyring',
            '--name=client.bootstrap-osd',
            '--add-key={}'.format(key)
        ]
        subprocess.check_call(cmd)


def import_osd_upgrade_key(key):
    if not os.path.exists(_upgrade_keyring):
        cmd = [
            "sudo",
            "-u",
            ceph_user(),
            'ceph-authtool',
            _upgrade_keyring,
            '--create-keyring',
            '--name=client.osd-upgrade',
            '--add-key={}'.format(key)
        ]
        subprocess.check_call(cmd)


def generate_monitor_secret():
    cmd = [
        'ceph-authtool',
        '/dev/stdout',
        '--name=mon.',
        '--gen-key'
    ]
    res = check_output(cmd)

    return "{}==".format(res.split('=')[1].strip())

# OSD caps taken from ceph-create-keys
_osd_bootstrap_caps = {
    'mon': [
        'allow command osd create ...',
        'allow command osd crush set ...',
        r'allow command auth add * osd allow\ * mon allow\ rwx',
        'allow command mon getmap'
    ]
}

_osd_bootstrap_caps_profile = {
    'mon': [
        'allow profile bootstrap-osd'
    ]
}


def parse_key(raw_key):
    # get-or-create appears to have different output depending
    # on whether its 'get' or 'create'
    # 'create' just returns the key, 'get' is more verbose and
    # needs parsing
    key = None
    if len(raw_key.splitlines()) == 1:
        key = raw_key
    else:
        for element in raw_key.splitlines():
            if 'key' in element:
                return element.split(' = ')[1].strip()  # IGNORE:E1103
    return key


def get_osd_bootstrap_key():
    try:
        # Attempt to get/create a key using the OSD bootstrap profile first
        key = get_named_key('bootstrap-osd',
                            _osd_bootstrap_caps_profile)
    except:
        # If that fails try with the older style permissions
        key = get_named_key('bootstrap-osd',
                            _osd_bootstrap_caps)
    return key


_radosgw_keyring = "/etc/ceph/keyring.rados.gateway"


def import_radosgw_key(key):
    if not os.path.exists(_radosgw_keyring):
        cmd = [
            "sudo",
            "-u",
            ceph_user(),
            'ceph-authtool',
            _radosgw_keyring,
            '--create-keyring',
            '--name=client.radosgw.gateway',
            '--add-key={}'.format(key)
        ]
        subprocess.check_call(cmd)

# OSD caps taken from ceph-create-keys
_radosgw_caps = {
    'mon': ['allow rw'],
    'osd': ['allow rwx']
}
_upgrade_caps = {
    'mon': ['allow rwx']
}


def get_radosgw_key(pool_list=None):
    return get_named_key(name='radosgw.gateway',
                         caps=_radosgw_caps,
                         pool_list=pool_list)


def get_mds_key(name):
    return create_named_keyring(entity='mds',
                                name=name,
                                caps=mds_caps)


_mds_bootstrap_caps_profile = {
    'mon': [
        'allow profile bootstrap-mds'
    ]
}


def get_mds_bootstrap_key():
    return get_named_key('bootstrap-mds',
                         _mds_bootstrap_caps_profile)


_default_caps = collections.OrderedDict([
    ('mon', ['allow r']),
    ('osd', ['allow rwx']),
])

admin_caps = collections.OrderedDict([
    ('mds', ['allow *']),
    ('mon', ['allow *']),
    ('osd', ['allow *'])
])

mds_caps = collections.OrderedDict([
    ('osd', ['allow *']),
    ('mds', ['allow']),
    ('mon', ['allow rwx']),
])

osd_upgrade_caps = collections.OrderedDict([
    ('mon', ['allow command "config-key"',
             'allow command "osd tree"',
             'allow command "config-key list"',
             'allow command "config-key put"',
             'allow command "config-key get"',
             'allow command "config-key exists"',
             'allow command "osd out"',
             'allow command "osd in"',
             'allow command "osd rm"',
             'allow command "auth del"',
             ])
])


def create_named_keyring(entity, name, caps=None):
    caps = caps or _default_caps
    cmd = [
        "sudo",
        "-u",
        ceph_user(),
        'ceph',
        '--name', 'mon.',
        '--keyring',
        '/var/lib/ceph/mon/ceph-{}/keyring'.format(
            socket.gethostname()
        ),
        'auth', 'get-or-create', '{entity}.{name}'.format(entity=entity,
                                                          name=name),
    ]
    for subsystem, subcaps in caps.items():
        cmd.extend([subsystem, '; '.join(subcaps)])
    log("Calling check_output: {}".format(cmd), level=DEBUG)
    return parse_key(check_output(cmd).strip())  # IGNORE:E1103


def get_upgrade_key():
    return get_named_key('upgrade-osd', _upgrade_caps)


def get_named_key(name, caps=None, pool_list=None):
    """
    Retrieve a specific named cephx key
    :param name: String Name of key to get.
    :param pool_list:  The list of pools to give access to
    :param caps:  dict of cephx capabilities
    :return: Returns a cephx key
    """
    try:
        # Does the key already exist?
        output = check_output(
            [
                'sudo',
                '-u', ceph_user(),
                'ceph',
                '--name', 'mon.',
                '--keyring',
                '/var/lib/ceph/mon/ceph-{}/keyring'.format(
                    socket.gethostname()
                ),
                'auth',
                'get',
                'client.{}'.format(name),
            ]).strip()
        return parse_key(output)
    except subprocess.CalledProcessError:
        # Couldn't get the key, time to create it!
        log("Creating new key for {}".format(name), level=DEBUG)
    caps = caps or _default_caps
    cmd = [
        "sudo",
        "-u",
        ceph_user(),
        'ceph',
        '--name', 'mon.',
        '--keyring',
        '/var/lib/ceph/mon/ceph-{}/keyring'.format(
            socket.gethostname()
        ),
        'auth', 'get-or-create', 'client.{}'.format(name),
    ]
    # Add capabilities
    for subsystem, subcaps in caps.items():
        if subsystem == 'osd':
            if pool_list:
                # This will output a string similar to:
                # "pool=rgw pool=rbd pool=something"
                pools = " ".join(['pool={0}'.format(i) for i in pool_list])
                subcaps[0] = subcaps[0] + " " + pools
        cmd.extend([subsystem, '; '.join(subcaps)])
    log("Calling check_output: {}".format(cmd), level=DEBUG)
    return parse_key(check_output(cmd).strip())  # IGNORE:E1103


def upgrade_key_caps(key, caps):
    """ Upgrade key to have capabilities caps """
    if not is_leader():
        # Not the MON leader OR not clustered
        return
    cmd = [
        "sudo", "-u", ceph_user(), 'ceph', 'auth', 'caps', key
    ]
    for subsystem, subcaps in caps.items():
        cmd.extend([subsystem, '; '.join(subcaps)])
    subprocess.check_call(cmd)


@cached
def systemd():
    return CompareHostReleases(lsb_release()['DISTRIB_CODENAME']) >= 'vivid'


def bootstrap_monitor_cluster(secret):
    hostname = socket.gethostname()
    path = '/var/lib/ceph/mon/ceph-{}'.format(hostname)
    done = '{}/done'.format(path)
    if systemd():
        init_marker = '{}/systemd'.format(path)
    else:
        init_marker = '{}/upstart'.format(path)

    keyring = '/var/lib/ceph/tmp/{}.mon.keyring'.format(hostname)

    if os.path.exists(done):
        log('bootstrap_monitor_cluster: mon already initialized.')
    else:
        # Ceph >= 0.61.3 needs this for ceph-mon fs creation
        mkdir('/var/run/ceph', owner=ceph_user(),
              group=ceph_user(), perms=0o755)
        mkdir(path, owner=ceph_user(), group=ceph_user())
        # end changes for Ceph >= 0.61.3
        try:
            subprocess.check_call(['ceph-authtool', keyring,
                                   '--create-keyring', '--name=mon.',
                                   '--add-key={}'.format(secret),
                                   '--cap', 'mon', 'allow *'])

            subprocess.check_call(['ceph-mon', '--mkfs',
                                   '-i', hostname,
                                   '--keyring', keyring])
            chownr(path, ceph_user(), ceph_user())
            with open(done, 'w'):
                pass
            with open(init_marker, 'w'):
                pass

            if systemd():
                subprocess.check_call(['systemctl', 'enable', 'ceph-mon'])
                service_restart('ceph-mon')
            else:
                service_restart('ceph-mon-all')
        except:
            raise
        finally:
            os.unlink(keyring)


def update_monfs():
    hostname = socket.gethostname()
    monfs = '/var/lib/ceph/mon/ceph-{}'.format(hostname)
    if systemd():
        init_marker = '{}/systemd'.format(monfs)
    else:
        init_marker = '{}/upstart'.format(monfs)
    if os.path.exists(monfs) and not os.path.exists(init_marker):
        # Mark mon as managed by upstart so that
        # it gets start correctly on reboots
        with open(init_marker, 'w'):
            pass


def maybe_zap_journal(journal_dev):
    if is_osd_disk(journal_dev):
        log('Looks like {} is already an OSD data'
            ' or journal, skipping.'.format(journal_dev))
        return
    zap_disk(journal_dev)
    log("Zapped journal device {}".format(journal_dev))


def get_partitions(dev):
    cmd = ['partx', '--raw', '--noheadings', dev]
    try:
        out = check_output(cmd).splitlines()
        log("get partitions: {}".format(out), level=DEBUG)
        return out
    except subprocess.CalledProcessError as e:
        log("Can't get info for {0}: {1}".format(dev, e.output))
        return []


def find_least_used_journal(journal_devices):
    usages = map(lambda a: (len(get_partitions(a)), a), journal_devices)
    least = min(usages, key=lambda t: t[0])
    return least[1]


def osdize(dev, osd_format, osd_journal, reformat_osd=False,
           ignore_errors=False, encrypt=False):
    if dev.startswith('/dev'):
        osdize_dev(dev, osd_format, osd_journal,
                   reformat_osd, ignore_errors, encrypt)
    else:
        osdize_dir(dev, encrypt)


def osdize_dev(dev, osd_format, osd_journal, reformat_osd=False,
               ignore_errors=False, encrypt=False):
    if not os.path.exists(dev):
        log('Path {} does not exist - bailing'.format(dev))
        return

    if not is_block_device(dev):
        log('Path {} is not a block device - bailing'.format(dev))
        return

    if is_osd_disk(dev) and not reformat_osd:
        log('Looks like {} is already an'
            ' OSD data or journal, skipping.'.format(dev))
        return

    if is_device_mounted(dev):
        log('Looks like {} is in use, skipping.'.format(dev))
        return

    status_set('maintenance', 'Initializing device {}'.format(dev))
    cmd = ['ceph-disk', 'prepare']
    # Later versions of ceph support more options
    if cmp_pkgrevno('ceph', '0.60') >= 0:
        if encrypt:
            cmd.append('--dmcrypt')
    if cmp_pkgrevno('ceph', '0.48.3') >= 0:
        if osd_format:
            cmd.append('--fs-type')
            cmd.append(osd_format)
        if reformat_osd:
            cmd.append('--zap-disk')
        cmd.append(dev)
        if osd_journal:
            least_used = find_least_used_journal(osd_journal)
            cmd.append(least_used)
    else:
        # Just provide the device - no other options
        # for older versions of ceph
        cmd.append(dev)
        if reformat_osd:
            zap_disk(dev)

    try:
        log("osdize cmd: {}".format(cmd))
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError:
        if ignore_errors:
            log('Unable to initialize device: {}'.format(dev), WARNING)
        else:
            log('Unable to initialize device: {}'.format(dev), ERROR)
            raise


def osdize_dir(path, encrypt=False):
    if os.path.exists(os.path.join(path, 'upstart')):
        log('Path {} is already configured as an OSD - bailing'.format(path))
        return

    if cmp_pkgrevno('ceph', "0.56.6") < 0:
        log('Unable to use directories for OSDs with ceph < 0.56.6',
            level=ERROR)
        return

    mkdir(path, owner=ceph_user(), group=ceph_user(), perms=0o755)
    chownr('/var/lib/ceph', ceph_user(), ceph_user())
    cmd = [
        'sudo', '-u', ceph_user(),
        'ceph-disk',
        'prepare',
        '--data-dir',
        path
    ]
    if cmp_pkgrevno('ceph', '0.60') >= 0:
        if encrypt:
            cmd.append('--dmcrypt')
    log("osdize dir cmd: {}".format(cmd))
    subprocess.check_call(cmd)


def filesystem_mounted(fs):
    return subprocess.call(['grep', '-wqs', fs, '/proc/mounts']) == 0


def get_running_osds():
    """Returns a list of the pids of the current running OSD daemons"""
    cmd = ['pgrep', 'ceph-osd']
    try:
        result = check_output(cmd)
        return result.split()
    except subprocess.CalledProcessError:
        return []


def get_cephfs(service):
    """
    List the Ceph Filesystems that exist
    :rtype : list.  Returns a list of the ceph filesystems
    :param service:  The service name to run the ceph command under
    """
    if get_version() < 0.86:
        # This command wasn't introduced until 0.86 ceph
        return []
    try:
        output = check_output(["ceph",
                               '--id', service,
                               "fs", "ls"])
        if not output:
            return []
        """
        Example subprocess output:
        'name: ip-172-31-23-165, metadata pool: ip-172-31-23-165_metadata,
         data pools: [ip-172-31-23-165_data ]\n'
        output: filesystems: ['ip-172-31-23-165']
        """
        filesystems = []
        for line in output.splitlines():
            parts = line.split(',')
            for part in parts:
                if "name" in part:
                    filesystems.append(part.split(' ')[1])
    except subprocess.CalledProcessError:
        return []


def wait_for_all_monitors_to_upgrade(new_version, upgrade_key):
    """
    Fairly self explanatory name.  This function will wait
    for all monitors in the cluster to upgrade or it will
    return after a timeout period has expired.
    :param new_version: str of the version to watch
    :param upgrade_key: the cephx key name to use
    """
    done = False
    start_time = time.time()
    monitor_list = []

    mon_map = get_mon_map('admin')
    if mon_map['monmap']['mons']:
        for mon in mon_map['monmap']['mons']:
            monitor_list.append(mon['name'])
    while not done:
        try:
            done = all(monitor_key_exists(upgrade_key, "{}_{}_{}_done".format(
                "mon", mon, new_version
            )) for mon in monitor_list)
            current_time = time.time()
            if current_time > (start_time + 10 * 60):
                raise Exception
            else:
                # Wait 30 seconds and test again if all monitors are upgraded
                time.sleep(30)
        except subprocess.CalledProcessError:
            raise


# Edge cases:
# 1. Previous node dies on upgrade, can we retry?
def roll_monitor_cluster(new_version, upgrade_key):
    """
    This is tricky to get right so here's what we're going to do.
    :param new_version: str of the version to upgrade to
    :param upgrade_key: the cephx key name to use when upgrading
    There's 2 possible cases: Either I'm first in line or not.
    If I'm not first in line I'll wait a random time between 5-30 seconds
    and test to see if the previous monitor is upgraded yet.
    """
    log('roll_monitor_cluster called with {}'.format(new_version))
    my_name = socket.gethostname()
    monitor_list = []
    mon_map = get_mon_map('admin')
    if mon_map['monmap']['mons']:
        for mon in mon_map['monmap']['mons']:
            monitor_list.append(mon['name'])
    else:
        status_set('blocked', 'Unable to get monitor cluster information')
        sys.exit(1)
    log('monitor_list: {}'.format(monitor_list))

    # A sorted list of osd unit names
    mon_sorted_list = sorted(monitor_list)

    try:
        position = mon_sorted_list.index(my_name)
        log("upgrade position: {}".format(position))
        if position == 0:
            # I'm first!  Roll
            # First set a key to inform others I'm about to roll
            lock_and_roll(upgrade_key=upgrade_key,
                          service='mon',
                          my_name=my_name,
                          version=new_version)
        else:
            # Check if the previous node has finished
            status_set('waiting',
                       'Waiting on {} to finish upgrading'.format(
                           mon_sorted_list[position - 1]))
            wait_on_previous_node(upgrade_key=upgrade_key,
                                  service='mon',
                                  previous_node=mon_sorted_list[position - 1],
                                  version=new_version)
            lock_and_roll(upgrade_key=upgrade_key,
                          service='mon',
                          my_name=my_name,
                          version=new_version)
    except ValueError:
        log("Failed to find {} in list {}.".format(
            my_name, mon_sorted_list))
        status_set('blocked', 'failed to upgrade monitor')


def upgrade_monitor(new_version):
    current_version = get_version()
    status_set("maintenance", "Upgrading monitor")
    log("Current ceph version is {}".format(current_version))
    log("Upgrading to: {}".format(new_version))

    try:
        add_source(config('source'), config('key'))
        apt_update(fatal=True)
    except subprocess.CalledProcessError as err:
        log("Adding the ceph source failed with message: {}".format(
            err.message))
        status_set("blocked", "Upgrade to {} failed".format(new_version))
        sys.exit(1)
    try:
        if systemd():
            for mon_id in get_local_mon_ids():
                service_stop('ceph-mon@{}'.format(mon_id))
        else:
            service_stop('ceph-mon-all')
        apt_install(packages=PACKAGES, fatal=True)

        # Ensure the files and directories under /var/lib/ceph is chowned
        # properly as part of the move to the Jewel release, which moved the
        # ceph daemons to running as ceph:ceph instead of root:root.
        if new_version == 'jewel':
            # Ensure the ownership of Ceph's directories is correct
            owner = ceph_user()
            chownr(path=os.path.join(os.sep, "var", "lib", "ceph"),
                   owner=owner,
                   group=owner,
                   follow_links=True)

        if systemd():
            for mon_id in get_local_mon_ids():
                service_start('ceph-mon@{}'.format(mon_id))
        else:
            service_start('ceph-mon-all')
    except subprocess.CalledProcessError as err:
        log("Stopping ceph and upgrading packages failed "
            "with message: {}".format(err.message))
        status_set("blocked", "Upgrade to {} failed".format(new_version))
        sys.exit(1)


def lock_and_roll(upgrade_key, service, my_name, version):
    start_timestamp = time.time()

    log('monitor_key_set {}_{}_{}_start {}'.format(
        service,
        my_name,
        version,
        start_timestamp))
    monitor_key_set(upgrade_key, "{}_{}_{}_start".format(
        service, my_name, version), start_timestamp)
    log("Rolling")

    # This should be quick
    if service == 'osd':
        upgrade_osd(version)
    elif service == 'mon':
        upgrade_monitor(version)
    else:
        log("Unknown service {}.  Unable to upgrade".format(service),
            level=ERROR)
    log("Done")

    stop_timestamp = time.time()
    # Set a key to inform others I am finished
    log('monitor_key_set {}_{}_{}_done {}'.format(service,
                                                  my_name,
                                                  version,
                                                  stop_timestamp))
    status_set('maintenance', 'Finishing upgrade')
    monitor_key_set(upgrade_key, "{}_{}_{}_done".format(service,
                                                        my_name,
                                                        version),
                    stop_timestamp)


def wait_on_previous_node(upgrade_key, service, previous_node, version):
    log("Previous node is: {}".format(previous_node))

    previous_node_finished = monitor_key_exists(
        upgrade_key,
        "{}_{}_{}_done".format(service, previous_node, version))

    while previous_node_finished is False:
        log("{} is not finished. Waiting".format(previous_node))
        # Has this node been trying to upgrade for longer than
        # 10 minutes?
        # If so then move on and consider that node dead.

        # NOTE: This assumes the clusters clocks are somewhat accurate
        # If the hosts clock is really far off it may cause it to skip
        # the previous node even though it shouldn't.
        current_timestamp = time.time()
        previous_node_start_time = monitor_key_get(
            upgrade_key,
            "{}_{}_{}_start".format(service, previous_node, version))
        if (current_timestamp - (10 * 60)) > previous_node_start_time:
            # Previous node is probably dead.  Lets move on
            if previous_node_start_time is not None:
                log(
                    "Waited 10 mins on node {}. current time: {} > "
                    "previous node start time: {} Moving on".format(
                        previous_node,
                        (current_timestamp - (10 * 60)),
                        previous_node_start_time))
                return
        else:
            # I have to wait.  Sleep a random amount of time and then
            # check if I can lock,upgrade and roll.
            wait_time = random.randrange(5, 30)
            log('waiting for {} seconds'.format(wait_time))
            time.sleep(wait_time)
            previous_node_finished = monitor_key_exists(
                upgrade_key,
                "{}_{}_{}_done".format(service, previous_node, version))


def get_upgrade_position(osd_sorted_list, match_name):
    for index, item in enumerate(osd_sorted_list):
        if item.name == match_name:
            return index
    return None


# Edge cases:
# 1. Previous node dies on upgrade, can we retry?
# 2. This assumes that the osd failure domain is not set to osd.
#    It rolls an entire server at a time.
def roll_osd_cluster(new_version, upgrade_key):
    """
    This is tricky to get right so here's what we're going to do.
    :param new_version: str of the version to upgrade to
    :param upgrade_key: the cephx key name to use when upgrading
    There's 2 possible cases: Either I'm first in line or not.
    If I'm not first in line I'll wait a random time between 5-30 seconds
    and test to see if the previous osd is upgraded yet.

    TODO: If you're not in the same failure domain it's safe to upgrade
     1. Examine all pools and adopt the most strict failure domain policy
        Example: Pool 1: Failure domain = rack
        Pool 2: Failure domain = host
        Pool 3: Failure domain = row

        outcome: Failure domain = host
    """
    log('roll_osd_cluster called with {}'.format(new_version))
    my_name = socket.gethostname()
    osd_tree = get_osd_tree(service=upgrade_key)
    # A sorted list of osd unit names
    osd_sorted_list = sorted(osd_tree)
    log("osd_sorted_list: {}".format(osd_sorted_list))

    try:
        position = get_upgrade_position(osd_sorted_list, my_name)
        log("upgrade position: {}".format(position))
        if position == 0:
            # I'm first!  Roll
            # First set a key to inform others I'm about to roll
            lock_and_roll(upgrade_key=upgrade_key,
                          service='osd',
                          my_name=my_name,
                          version=new_version)
        else:
            # Check if the previous node has finished
            status_set('blocked',
                       'Waiting on {} to finish upgrading'.format(
                           osd_sorted_list[position - 1].name))
            wait_on_previous_node(
                upgrade_key=upgrade_key,
                service='osd',
                previous_node=osd_sorted_list[position - 1].name,
                version=new_version)
            lock_and_roll(upgrade_key=upgrade_key,
                          service='osd',
                          my_name=my_name,
                          version=new_version)
    except ValueError:
        log("Failed to find name {} in list {}".format(
            my_name, osd_sorted_list))
        status_set('blocked', 'failed to upgrade osd')


def upgrade_osd(new_version):
    current_version = get_version()
    status_set("maintenance", "Upgrading osd")
    log("Current ceph version is {}".format(current_version))
    log("Upgrading to: {}".format(new_version))

    try:
        add_source(config('source'), config('key'))
        apt_update(fatal=True)
    except subprocess.CalledProcessError as err:
        log("Adding the ceph sources failed with message: {}".format(
            err.message))
        status_set("blocked", "Upgrade to {} failed".format(new_version))
        sys.exit(1)

    try:
        # Upgrade the packages before restarting the daemons.
        status_set('maintenance', 'Upgrading packages to %s' % new_version)
        apt_install(packages=PACKAGES, fatal=True)

        # If the upgrade does not need an ownership update of any of the
        # directories in the osd service directory, then simply restart
        # all of the OSDs at the same time as this will be the fastest
        # way to update the code on the node.
        if not dirs_need_ownership_update('osd'):
            log('Restarting all OSDs to load new binaries', DEBUG)
            service_restart('ceph-osd-all')
            return

        # Need to change the ownership of all directories which are not OSD
        # directories as well.
        # TODO - this should probably be moved to the general upgrade function
        #        and done before mon/osd.
        update_owner(CEPH_BASE_DIR, recurse_dirs=False)
        non_osd_dirs = filter(lambda x: not x == 'osd',
                              os.listdir(CEPH_BASE_DIR))
        non_osd_dirs = map(lambda x: os.path.join(CEPH_BASE_DIR, x),
                           non_osd_dirs)
        for path in non_osd_dirs:
            update_owner(path)

        # Fast service restart wasn't an option because each of the OSD
        # directories need the ownership updated for all the files on
        # the OSD. Walk through the OSDs one-by-one upgrading the OSD.
        for osd_dir in _get_child_dirs(OSD_BASE_DIR):
            try:
                osd_num = _get_osd_num_from_dirname(osd_dir)
                _upgrade_single_osd(osd_num, osd_dir)
            except ValueError as ex:
                # Directory could not be parsed - junk directory?
                log('Could not parse osd directory %s: %s' % (osd_dir, ex),
                    WARNING)
                continue

    except (subprocess.CalledProcessError, IOError) as err:
        log("Stopping ceph and upgrading packages failed "
            "with message: {}".format(err.message))
        status_set("blocked", "Upgrade to {} failed".format(new_version))
        sys.exit(1)


def _upgrade_single_osd(osd_num, osd_dir):
    """Upgrades the single OSD directory.

    :param osd_num: the num of the OSD
    :param osd_dir: the directory of the OSD to upgrade
    :raises CalledProcessError: if an error occurs in a command issued as part
                                of the upgrade process
    :raises IOError: if an error occurs reading/writing to a file as part
                     of the upgrade process
    """
    stop_osd(osd_num)
    disable_osd(osd_num)
    update_owner(osd_dir)
    enable_osd(osd_num)
    start_osd(osd_num)


def stop_osd(osd_num):
    """Stops the specified OSD number.

    :param osd_num: the osd number to stop
    """
    if systemd():
        service_stop('ceph-osd@{}'.format(osd_num))
    else:
        service_stop('ceph-osd', id=osd_num)


def start_osd(osd_num):
    """Starts the specified OSD number.

    :param osd_num: the osd number to start.
    """
    if systemd():
        service_start('ceph-osd@{}'.format(osd_num))
    else:
        service_start('ceph-osd', id=osd_num)


def disable_osd(osd_num):
    """Disables the specified OSD number.

    Ensures that the specified osd will not be automatically started at the
    next reboot of the system. Due to differences between init systems,
    this method cannot make any guarantees that the specified osd cannot be
    started manually.

    :param osd_num: the osd id which should be disabled.
    :raises CalledProcessError: if an error occurs invoking the systemd cmd
                                to disable the OSD
    :raises IOError, OSError: if the attempt to read/remove the ready file in
                              an upstart enabled system fails
    """
    if systemd():
        # When running under systemd, the individual ceph-osd daemons run as
        # templated units and can be directly addressed by referring to the
        # templated service name ceph-osd@<osd_num>. Additionally, systemd
        # allows one to disable a specific templated unit by running the
        # 'systemctl disable ceph-osd@<osd_num>' command. When disabled, the
        # OSD should remain disabled until re-enabled via systemd.
        # Note: disabling an already disabled service in systemd returns 0, so
        # no need to check whether it is enabled or not.
        cmd = ['systemctl', 'disable', 'ceph-osd@{}'.format(osd_num)]
        subprocess.check_call(cmd)
    else:
        # Neither upstart nor the ceph-osd upstart script provides for
        # disabling the starting of an OSD automatically. The specific OSD
        # cannot be prevented from running manually, however it can be
        # prevented from running automatically on reboot by removing the
        # 'ready' file in the OSD's root directory. This is due to the
        # ceph-osd-all upstart script checking for the presence of this file
        # before starting the OSD.
        ready_file = os.path.join(OSD_BASE_DIR, 'ceph-{}'.format(osd_num),
                                  'ready')
        if os.path.exists(ready_file):
            os.unlink(ready_file)


def enable_osd(osd_num):
    """Enables the specified OSD number.

    Ensures that the specified osd_num will be enabled and ready to start
    automatically in the event of a reboot.

    :param osd_num: the osd id which should be enabled.
    :raises CalledProcessError: if the call to the systemd command issued
                                fails when enabling the service
    :raises IOError: if the attempt to write the ready file in an usptart
                     enabled system fails
    """
    if systemd():
        cmd = ['systemctl', 'enable', 'ceph-osd@{}'.format(osd_num)]
        subprocess.check_call(cmd)
    else:
        # When running on upstart, the OSDs are started via the ceph-osd-all
        # upstart script which will only start the osd if it has a 'ready'
        # file. Make sure that file exists.
        ready_file = os.path.join(OSD_BASE_DIR, 'ceph-{}'.format(osd_num),
                                  'ready')
        with open(ready_file, 'w') as f:
            f.write('ready')

        # Make sure the correct user owns the file. It shouldn't be necessary
        # as the upstart script should run with root privileges, but its better
        # to have all the files matching ownership.
        update_owner(ready_file)


def update_owner(path, recurse_dirs=True):
    """Changes the ownership of the specified path.

    Changes the ownership of the specified path to the new ceph daemon user
    using the system's native chown functionality. This may take awhile,
    so this method will issue a set_status for any changes of ownership which
    recurses into directory structures.

    :param path: the path to recursively change ownership for
    :param recurse_dirs: boolean indicating whether to recursively change the
                         ownership of all the files in a path's subtree or to
                         simply change the ownership of the path.
    :raises CalledProcessError: if an error occurs issuing the chown system
                                command
    """
    user = ceph_user()
    user_group = '{ceph_user}:{ceph_user}'.format(ceph_user=user)
    cmd = ['chown', user_group, path]
    if os.path.isdir(path) and recurse_dirs:
        status_set('maintenance', ('Updating ownership of %s to %s' %
                                   (path, user)))
        cmd.insert(1, '-R')

    log('Changing ownership of {path} to {user}'.format(
        path=path, user=user_group), DEBUG)
    start = datetime.now()
    subprocess.check_call(cmd)
    elapsed_time = (datetime.now() - start)

    log('Took {secs} seconds to change the ownership of path: {path}'.format(
        secs=elapsed_time.total_seconds(), path=path), DEBUG)


def list_pools(service):
    """
    This will list the current pools that Ceph has

    :param service: String service id to run under
    :return: list.  Returns a list of the ceph pools. Raises CalledProcessError
    if the subprocess fails to run.
    """
    try:
        pool_list = []
        pools = check_output(['rados', '--id', service, 'lspools'])
        for pool in pools.splitlines():
            pool_list.append(pool)
        return pool_list
    except subprocess.CalledProcessError as err:
        log("rados lspools failed with error: {}".format(err.output))
        raise


def dirs_need_ownership_update(service):
    """Determines if directories still need change of ownership.

    Examines the set of directories under the /var/lib/ceph/{service} directory
    and determines if they have the correct ownership or not. This is
    necessary due to the upgrade from Hammer to Jewel where the daemon user
    changes from root: to ceph:.

    :param service: the name of the service folder to check (e.g. osd, mon)
    :return: boolean. True if the directories need a change of ownership,
             False otherwise.
    :raises IOError: if an error occurs reading the file stats from one of
                     the child directories.
    :raises OSError: if the specified path does not exist or some other error
    """
    expected_owner = expected_group = ceph_user()
    path = os.path.join(CEPH_BASE_DIR, service)
    for child in _get_child_dirs(path):
        curr_owner, curr_group = owner(child)

        if (curr_owner == expected_owner) and (curr_group == expected_group):
            continue

        log('Directory "%s" needs its ownership updated' % child, DEBUG)
        return True

    # All child directories had the expected ownership
    return False

# A dict of valid ceph upgrade paths.  Mapping is old -> new
UPGRADE_PATHS = {
    'firefly': 'hammer',
    'hammer': 'jewel',
}

# Map UCA codenames to ceph codenames
UCA_CODENAME_MAP = {
    'icehouse': 'firefly',
    'juno': 'firefly',
    'kilo': 'hammer',
    'liberty': 'hammer',
    'mitaka': 'jewel',
}


def pretty_print_upgrade_paths():
    '''Pretty print supported upgrade paths for ceph'''
    lines = []
    for key, value in UPGRADE_PATHS.iteritems():
        lines.append("{} -> {}".format(key, value))
    return lines


def resolve_ceph_version(source):
    '''
    Resolves a version of ceph based on source configuration
    based on Ubuntu Cloud Archive pockets.

    @param: source: source configuration option of charm
    @returns: ceph release codename or None if not resolvable
    '''
    os_release = get_os_codename_install_source(source)
    return UCA_CODENAME_MAP.get(os_release)


def get_ceph_pg_stat():
    """
    Returns the result of ceph pg stat
    :return: dict
    """
    try:
        tree = check_output(['ceph', 'pg', 'stat', '--format=json'])
        try:
            json_tree = json.loads(tree)
            if not json_tree['num_pg_by_state']:
                return None
            return json_tree
        except ValueError as v:
            log("Unable to parse ceph pg stat json: {}. Error: {}".format(
                tree, v.message))
            raise
    except subprocess.CalledProcessError as e:
        log("ceph pg stat command failed with message: {}".format(
            e.message))
        raise


def get_ceph_health():
    """
    Returns the health of the cluster from a 'ceph health'
    :return: dict
      Also raises CalledProcessError if our ceph command fails
      To get the overall status, use get_ceph_health()['overall_status']
    """
    try:
        tree = check_output(
            ['ceph', 'health', '--format=json'])
        try:
            json_tree = json.loads(tree)
            # Make sure children are present in the json
            if not json_tree['overall_status']:
                return None
            return json_tree
        except ValueError as v:
            log("Unable to parse ceph tree json: {}. Error: {}".format(
                tree, v.message))
            raise
    except subprocess.CalledProcessError as e:
        log("ceph osd tree command failed with message: {}".format(
            e.message))
        raise


def reweight_osd(osd_num, new_weight):
    """
    Changes the crush weight of an OSD to the value specified.
    :param osd_num: the osd id which should be changed
    :param new_weight: the new weight for the OSD
    :returns: bool.  True if output looks right, else false.
    :raises CalledProcessError: if an error occurs invoking the systemd cmd
    """
    try:
        cmd_result = subprocess.check_output(
            ['ceph', 'osd', 'crush', 'reweight', "osd.{}".format(osd_num),
             new_weight], stderr=subprocess.STDOUT)
        expected_result = "reweighted item id {ID} name \'osd.{ID}\'".format(
                          ID=osd_num) + " to {}".format(new_weight)
        log(cmd_result)
        if expected_result in cmd_result:
            return True
        return False
    except subprocess.CalledProcessError as e:
        log("ceph osd tree command failed with message: {}".format(
            e.message))
        raise
