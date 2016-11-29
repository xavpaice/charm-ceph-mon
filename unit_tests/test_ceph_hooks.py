import copy
import unittest
import sys

from mock import patch, MagicMock, DEFAULT

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
                'prefer-ipv6': False}


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
                upgrade_keys=DEFAULT,
                mon_relation_joined=DEFAULT,
                is_relation_made=DEFAULT) as mocks, patch(
                    "charmhelpers.contrib.hardening.harden.config"):
            mocks["is_relation_made"].return_value = True
            ceph_hooks.upgrade_charm()
        mocks["apt_install"].assert_called_with(
            ["python-dbus", "lockfile-progs"])
