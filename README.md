# Overview

Ceph is a distributed storage and network file system designed to provide
excellent performance, reliability, and scalability.

This charm deploys a Ceph monitor cluster.

# Usage

Boot things up by using:

    juju deploy -n 3 ceph-mon

By default the ceph-mon cluster will not bootstrap until 3 service units have
been deployed and started; this is to ensure that a quorum is achieved prior to
adding storage devices.

## Actions

This charm supports pausing and resuming ceph's health functions on a cluster, for example when doing maintenance on a machine. to pause or resume, call:

`juju action do --unit ceph-mon/0 pause-health` or `juju action do --unit ceph-mon/0 resume-health`

## Scale Out Usage

You can use the Ceph OSD and Ceph Radosgw charms:

- [Ceph OSD](https://jujucharms.com/ceph-osd)
- [Ceph Rados Gateway](https://jujucharms.com/ceph-radosgw)

## Rolling Upgrades

ceph-mon and ceph-osd charms have the ability to initiate a rolling upgrade.
This is initiated by setting the config value for `source`.  To perform a
rolling upgrade first set the source for ceph-mon.  Watch `juju status`.
Once the monitor cluster is upgraded proceed to setting the ceph-osd source
setting.  Again watch `juju status` for output.  The monitors and osds will
sort themselves into a known order and upgrade one by one.  As each server is
upgrading the upgrade code will down all the monitor or osd processes on that
server, apply the update and then restart them. You will notice in the
`juju status` output that the servers will tell you which previous server they
are waiting on.

#### Supported Upgrade Paths
Currently the following upgrade paths are supported using 
the [Ubuntu Cloud Archive](https://wiki.ubuntu.com/OpenStack/CloudArchive):
- trusty-firefly -> trusty-hammer
- trusty-hammer -> trusty-jewel

Firefly is available in Trusty, Hammer is in Trusty-Juno (end of life),
Trusty-Kilo, Trusty-Liberty, and Jewel is available in Trusty-Mitaka.

For example if the current config source setting is: `cloud:trusty-liberty`
changing that to `cloud:trusty-mitaka` will initiate a rolling upgrade of 
the monitor cluster from hammer to jewel.

#### Edge cases
There's an edge case in the upgrade code where if the previous node never
starts upgrading itself then the rolling upgrade can hang forever.  If you
notice this has happened it can be fixed by setting the appropriate key in the
ceph monitor cluster. The monitor cluster will have
keys that look like `ceph-mon_ip-ceph-mon-0_1484680239.573482_start` and
`ceph-mon_ip-ceph-mon-0_1484680274.181742_stop`. What each server is looking for
is that stop key to indicate that the previous server upgraded successfully and
it's safe to take itself down.  If the stop key is not present it will wait
10 minutes, then consider that server dead and move on.

## Network Space support

This charm supports the use of Juju Network Spaces, allowing the charm to be bound to network space configurations managed directly by Juju.  This is only supported with Juju 2.0 and above.

Network traffic can be bound to specific network spaces using the public (front-side) and cluster (back-side) bindings:

    juju deploy ceph-mon --bind "public=data-space cluster=cluster-space"

alternatively these can also be provided as part of a Juju native bundle configuration:

    ceph-mon:
      charm: cs:xenial/ceph-mon
      num_units: 1
      bindings:
        public: data-space
        cluster: cluster-space

Please refer to the [Ceph Network Reference](http://docs.ceph.com/docs/master/rados/configuration/network-config-ref) for details on how using these options effects network traffic within a Ceph deployment.

**NOTE:** Spaces must be configured in the underlying provider prior to attempting to use them.

**NOTE**: Existing deployments using ceph-*-network configuration options will continue to function; these options are preferred over any network space binding provided if set.

**NOTE**: The monitor-hosts field is only used to migrate existing clusters to a juju managed solution and should be left blank otherwise.

# Contact Information

## Authors

- Paul Collins <paul.collins@canonical.com>,
- James Page <james.page@ubuntu.com>

Report bugs on [Launchpad](http://bugs.launchpad.net/charms/+source/ceph/+filebug)

## Ceph

- [Ceph website](http://ceph.com)
- [Ceph mailing lists](http://ceph.com/resources/mailing-list-irc/)
- [Ceph bug tracker](http://tracker.ceph.com/projects/ceph)

# Technical Footnotes

This charm uses the new-style Ceph deployment as reverse-engineered from the
Chef cookbook at https://github.com/ceph/ceph-cookbooks, although we selected
a different strategy to form the monitor cluster. Since we don't know the
names *or* addresses of the machines in advance, we use the _relation-joined_
hook to wait for all three nodes to come up, and then write their addresses
to ceph.conf in the "mon host" parameter. After we initialize the monitor
cluster a quorum forms quickly, and OSD bringup proceeds.

See [the documentation](http://ceph.com/docs/master/dev/mon-bootstrap/) for more information on Ceph monitor cluster deployment strategies and pitfalls.
