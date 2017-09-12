import copy
import unittest
import sys

from mock import patch, MagicMock, DEFAULT, call

# python-apt is not installed as part of test-requirements but is imported by
# some charmhelpers modules so create a fake import.
mock_apt = MagicMock()
sys.modules['apt'] = mock_apt
mock_apt.apt_pkg = MagicMock()

import charmhelpers.contrib.storage.linux.ceph as ceph
import ceph_hooks


CHARM_CONFIG = {'config-flags': '',
                'auth-supported': False,
                'fsid': '1234',
                'loglevel': 1,
                'use-syslog': True,
                'osd-journal-size': 1024,
                'use-direct-io': True,
                'osd-format': 'ext4',
                'monitor-hosts': '',
                'prefer-ipv6': False,
                'default-rbd-features': None}


class CephHooksTestCase(unittest.TestCase):
    def setUp(self):
        super(CephHooksTestCase, self).setUp()

    @patch.object(ceph_hooks, 'get_public_addr', lambda *args: "10.0.0.1")
    @patch.object(ceph_hooks, 'get_cluster_addr', lambda *args: "10.1.0.1")
    @patch.object(ceph_hooks, 'cmp_pkgrevno', lambda *args: 1)
    @patch.object(ceph_hooks, 'get_mon_hosts', lambda *args: ['10.0.0.1',
                                                              '10.0.0.2'])
    @patch.object(ceph_hooks, 'get_networks', lambda *args: "")
    @patch.object(ceph_hooks, 'leader_get', lambda *args: '1234')
    @patch.object(ceph, 'config')
    @patch.object(ceph_hooks, 'config')
    def test_get_ceph_context(self, mock_config, mock_config2):
        config = copy.deepcopy(CHARM_CONFIG)
        mock_config.side_effect = lambda key: config[key]
        mock_config2.side_effect = lambda key: config[key]
        ctxt = ceph_hooks.get_ceph_context()
        expected = {'auth_supported': False,
                    'ceph_cluster_network': '',
                    'ceph_public_network': '',
                    'cluster_addr': '10.1.0.1',
                    'dio': 'true',
                    'fsid': '1234',
                    'loglevel': 1,
                    'mon_hosts': '10.0.0.1 10.0.0.2',
                    'old_auth': False,
                    'public_addr': '10.0.0.1',
                    'use_syslog': 'true'}
        self.assertEqual(ctxt, expected)

    @patch.object(ceph_hooks, 'get_public_addr', lambda *args: "10.0.0.1")
    @patch.object(ceph_hooks, 'get_cluster_addr', lambda *args: "10.1.0.1")
    @patch.object(ceph_hooks, 'cmp_pkgrevno',
                  lambda pkg, ver: -1 if ver == '12.1.0' else 1)
    @patch.object(ceph_hooks, 'get_mon_hosts', lambda *args: ['10.0.0.1',
                                                              '10.0.0.2'])
    @patch.object(ceph_hooks, 'get_networks', lambda *args: "")
    @patch.object(ceph_hooks, 'leader_get', lambda *args: '1234')
    @patch.object(ceph, 'config')
    @patch.object(ceph_hooks, 'config')
    def test_get_ceph_context_rbd_features(self, mock_config, mock_config2):
        config = copy.deepcopy(CHARM_CONFIG)
        config['default-rbd-features'] = 1
        mock_config.side_effect = lambda key: config[key]
        mock_config2.side_effect = lambda key: config[key]
        ctxt = ceph_hooks.get_ceph_context()
        expected = {'auth_supported': False,
                    'ceph_cluster_network': '',
                    'ceph_public_network': '',
                    'cluster_addr': '10.1.0.1',
                    'dio': 'true',
                    'fsid': '1234',
                    'loglevel': 1,
                    'mon_hosts': '10.0.0.1 10.0.0.2',
                    'old_auth': False,
                    'public_addr': '10.0.0.1',
                    'use_syslog': 'true',
                    'rbd_features': 1}
        self.assertEqual(ctxt, expected)

    @patch.object(ceph_hooks, 'get_public_addr', lambda *args: "10.0.0.1")
    @patch.object(ceph_hooks, 'get_cluster_addr', lambda *args: "10.1.0.1")
    @patch.object(ceph_hooks, 'cmp_pkgrevno', lambda *args: 1)
    @patch.object(ceph_hooks, 'get_mon_hosts', lambda *args: ['10.0.0.1',
                                                              '10.0.0.2'])
    @patch.object(ceph_hooks, 'get_networks', lambda *args: "")
    @patch.object(ceph_hooks, 'leader_get', lambda *args: '1234')
    @patch.object(ceph, 'config')
    @patch.object(ceph_hooks, 'config')
    def test_get_ceph_context_w_config_flags(self, mock_config, mock_config2):
        config = copy.deepcopy(CHARM_CONFIG)
        config['config-flags'] = '{"mon": {"mon sync max retries": 10}}'
        mock_config.side_effect = lambda key: config[key]
        mock_config2.side_effect = lambda key: config[key]
        ctxt = ceph_hooks.get_ceph_context()
        expected = {'auth_supported': False,
                    'ceph_cluster_network': '',
                    'ceph_public_network': '',
                    'cluster_addr': '10.1.0.1',
                    'dio': 'true',
                    'fsid': '1234',
                    'loglevel': 1,
                    'mon_hosts': '10.0.0.1 10.0.0.2',
                    'old_auth': False,
                    'mon': {'mon sync max retries': 10},
                    'public_addr': '10.0.0.1',
                    'use_syslog': 'true'}
        self.assertEqual(ctxt, expected)

    @patch.object(ceph_hooks, 'get_public_addr', lambda *args: "10.0.0.1")
    @patch.object(ceph_hooks, 'get_cluster_addr', lambda *args: "10.1.0.1")
    @patch.object(ceph_hooks, 'cmp_pkgrevno', lambda *args: 1)
    @patch.object(ceph_hooks, 'get_mon_hosts', lambda *args: ['10.0.0.1',
                                                              '10.0.0.2'])
    @patch.object(ceph_hooks, 'get_networks', lambda *args: "")
    @patch.object(ceph_hooks, 'leader_get', lambda *args: '1234')
    @patch.object(ceph, 'config')
    @patch.object(ceph_hooks, 'config')
    def test_get_ceph_context_w_config_flags_invalid(self, mock_config,
                                                     mock_config2):
        config = copy.deepcopy(CHARM_CONFIG)
        config['config-flags'] = ('{"mon": {"mon sync max retries": 10},'
                                  '"foo": "bar"}')
        mock_config.side_effect = lambda key: config[key]
        mock_config2.side_effect = lambda key: config[key]
        ctxt = ceph_hooks.get_ceph_context()
        expected = {'auth_supported': False,
                    'ceph_cluster_network': '',
                    'ceph_public_network': '',
                    'cluster_addr': '10.1.0.1',
                    'dio': 'true',
                    'fsid': '1234',
                    'loglevel': 1,
                    'mon_hosts': '10.0.0.1 10.0.0.2',
                    'old_auth': False,
                    'mon': {'mon sync max retries': 10},
                    'public_addr': '10.0.0.1',
                    'use_syslog': 'true'}
        self.assertEqual(ctxt, expected)

    def test_nrpe_dependency_installed(self):
        with patch.multiple(ceph_hooks,
                            apt_install=DEFAULT,
                            rsync=DEFAULT,
                            log=DEFAULT,
                            write_file=DEFAULT,
                            nrpe=DEFAULT) as mocks:
            ceph_hooks.update_nrpe_config()
        mocks["apt_install"].assert_called_once_with(
            ["python-dbus", "lockfile-progs"])

    def test_upgrade_charm_with_nrpe_relation_installs_dependencies(self):
        with patch.multiple(
                ceph_hooks,
                apt_install=DEFAULT,
                rsync=DEFAULT,
                log=DEFAULT,
                write_file=DEFAULT,
                nrpe=DEFAULT,
                emit_cephconf=DEFAULT,
                mon_relation_joined=DEFAULT,
                is_relation_made=DEFAULT) as mocks, patch(
                    "charmhelpers.contrib.hardening.harden.config"):
            mocks["is_relation_made"].return_value = True
            ceph_hooks.upgrade_charm()
        mocks["apt_install"].assert_called_with(
            ["python-dbus", "lockfile-progs"])


class RelatedUnitsTestCase(unittest.TestCase):

    _units = {
        'osd:0': ['ceph-osd-a/0',
                  'ceph-osd-a/1',
                  'ceph-osd-a/2'],
        'osd:23': ['ceph-osd-b/1',
                   'ceph-osd-b/2',
                   'ceph-osd-b/3'],
    }

    def setUp(self):
        super(RelatedUnitsTestCase, self).setUp()

    @patch.object(ceph_hooks, 'relation_ids')
    @patch.object(ceph_hooks, 'related_units')
    def test_related_ods_single_relation(self,
                                         related_units,
                                         relation_ids):
        relation_ids.return_value = ['osd:0']
        related_units.side_effect = lambda x: self._units.get(x)
        self.assertTrue(ceph_hooks.related_osds())
        self.assertFalse(ceph_hooks.related_osds(6))
        relation_ids.assert_called_with('osd')
        related_units.assert_called_with('osd:0')

    @patch.object(ceph_hooks, 'relation_ids')
    @patch.object(ceph_hooks, 'related_units')
    def test_related_ods_multi_relation(self,
                                        related_units,
                                        relation_ids):
        relation_ids.return_value = ['osd:0', 'osd:23']
        related_units.side_effect = lambda x: self._units.get(x)
        self.assertTrue(ceph_hooks.related_osds())
        self.assertTrue(ceph_hooks.related_osds(6))
        self.assertFalse(ceph_hooks.related_osds(9))
        relation_ids.assert_called_with('osd')
        related_units.assert_has_calls([
            call('osd:0'),
            call('osd:23')
        ])
