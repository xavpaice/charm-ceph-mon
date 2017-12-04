# Copyright 2016 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from subprocess import CalledProcessError, check_output
import rados
import sys

sys.path.append('hooks')

from charmhelpers.core.hookenv import log, action_get, action_fail
from charmhelpers.contrib.storage.linux.ceph import pool_set, \
    set_pool_quota, snapshot_pool, remove_pool_snapshot


# Connect to Ceph via Librados and return a connection
def connect():
    """Creates a connection to Ceph using librados."""
    try:
        cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
        cluster.connect()
        return cluster
    except (rados.IOError,
            rados.ObjectNotFound,
            rados.NoData,
            rados.NoSpace,
            rados.PermissionError) as rados_error:
        log("librados failed with error: {}".format(str(rados_error)))


def create_crush_rule():
    """Stub function."""
    # Shell out
    pass


def list_pools():
    """Return a list of all Ceph pools."""
    try:
        cluster = connect()
        pool_list = cluster.list_pools()
        cluster.shutdown()
        return pool_list
    except (rados.IOError,
            rados.ObjectNotFound,
            rados.NoData,
            rados.NoSpace,
            rados.PermissionError) as e:
        action_fail(str(e))


def get_health():
    """
    Returns the output of 'ceph health'.

    On error, 'unknown' is returned.
    """
    try:
        value = check_output(['ceph', 'health'])
        return value
    except CalledProcessError as e:
        action_fail(e.message)
        return 'Getting health failed, health unknown'


def pool_get():
    """
    Returns a key from a pool using 'ceph osd pool get'.

    The key is provided via the 'key' action parameter and the
    pool provided by the 'pool_name' parameter. These are used when
    running 'ceph osd pool get <pool_name> <key>', the result of
    which is returned.

    On failure, 'unknown' will be returned.
    """
    key = action_get("key")
    pool_name = action_get("pool_name")
    try:
        value = (check_output(['ceph', 'osd', 'pool', 'get', pool_name, key])
                 .decode('UTF-8'))
        return value
    except CalledProcessError as e:
        action_fail(str(e))
        return 'unknown'


def set_pool():
    """
    Sets an arbitrary key key in a Ceph pool.

    Sets the key specified by the action parameter 'key' to the value
    specified in the action parameter 'value' for the pool specified
    by the action parameter 'pool_name' using the charmhelpers
    'pool_set' function.
    """
    key = action_get("key")
    value = action_get("value")
    pool_name = action_get("pool_name")
    pool_set(service='ceph', pool_name=pool_name, key=key, value=value)


def pool_stats():
    """
    Returns statistics for a pool.

    The pool name is provided by the action parameter 'pool-name'.
    """
    try:
        pool_name = action_get("pool-name")
        cluster = connect()
        ioctx = cluster.open_ioctx(pool_name)
        stats = ioctx.get_stats()
        ioctx.close()
        cluster.shutdown()
        return stats
    except (rados.Error,
            rados.IOError,
            rados.ObjectNotFound,
            rados.NoData,
            rados.NoSpace,
            rados.PermissionError) as e:
        action_fail(str(e))


def delete_pool_snapshot():
    """
    Delete a pool snapshot.

    Deletes a snapshot from the pool provided by the action
    parameter 'pool-name', with the snapshot name provided by
    action parameter 'snapshot-name'
    """
    pool_name = action_get("pool-name")
    snapshot_name = action_get("snapshot-name")
    remove_pool_snapshot(service='ceph',
                         pool_name=pool_name,
                         snapshot_name=snapshot_name)


# Note only one or the other can be set
def set_pool_max_bytes():
    """
    Sets the max bytes quota for a pool.

    Sets the pool quota maximum bytes for the pool specified by
    the action parameter 'pool-name' to the value specified by
    the action parameter 'max'
    """
    pool_name = action_get("pool-name")
    max_bytes = action_get("max")
    set_pool_quota(service='ceph',
                   pool_name=pool_name,
                   max_bytes=max_bytes)


def snapshot_ceph_pool():
    """
    Snapshots a Ceph pool.

    Snapshots the pool provided in action parameter 'pool-name' and
    uses the parameter provided in the action parameter 'snapshot-name'
    as the name for the snapshot.
    """
    pool_name = action_get("pool-name")
    snapshot_name = action_get("snapshot-name")
    snapshot_pool(service='ceph',
                  pool_name=pool_name,
                  snapshot_name=snapshot_name)
