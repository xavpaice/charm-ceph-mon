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

import unittest
import os
import sys

from mock import patch

# import the module we want to test
os.sys.path.insert(1, os.path.join(sys.path[0], 'files/nagios'))
import check_ceph_status


@patch('subprocess.check_output')
class NagiosTestCase(unittest.TestCase):

    def test_health_ok(self, mock_subprocess):
        with open('unit_tests/ceph_ok.json') as f:
            tree = f.read()
        mock_subprocess.return_value = tree.encode('UTF-8')
        args = check_ceph_status.parse_args(['--degraded_thresh', '1'])
        check_output = check_ceph_status.check_ceph_status(args)
        self.assertRegex(check_output, r"^All OK$")

    def test_health_warn(self, mock_subprocess):
        with open('unit_tests/ceph_warn.json') as f:
            tree = f.read()
        mock_subprocess.return_value = tree.encode('UTF-8')
        args = check_ceph_status.parse_args(['--degraded_thresh', '1'])
        self.assertRaises(check_ceph_status.WarnError,
                          lambda: check_ceph_status.check_ceph_status(args))

    def test_health_crit(self, mock_subprocess):
        with open('unit_tests/ceph_crit.json') as f:
            tree = f.read()
        mock_subprocess.return_value = tree.encode('UTF-8')
        args = check_ceph_status.parse_args(['--degraded_thresh', '1'])
        self.assertRaises(check_ceph_status.CriticalError,
                          lambda: check_ceph_status.check_ceph_status(args))

    def test_health_lotsdegraded(self, mock_subprocess):
        with open('unit_tests/ceph_params.json') as f:
            tree = f.read()
        mock_subprocess.return_value = tree.encode('UTF-8')
        args = check_ceph_status.parse_args(['--degraded_thresh', '1'])
        self.assertRaises(check_ceph_status.CriticalError,
                          lambda: check_ceph_status.check_ceph_status(args))

    def test_health_nodeepscrub(self, mock_subprocess):
        with open('unit_tests/ceph_nodeepscrub.json') as f:
            tree = f.read()
        mock_subprocess.return_value = tree.encode('UTF-8')
        args = check_ceph_status.parse_args(['--degraded_thresh', '1'])
        self.assertRaises(check_ceph_status.CriticalError,
                          lambda: check_ceph_status.check_ceph_status(args))

    def test_health_nodeepscrubok(self, mock_subprocess):
        with open('unit_tests/ceph_nodeepscrub.json') as f:
            tree = f.read()
        mock_subprocess.return_value = tree.encode('UTF-8')
        args = check_ceph_status.parse_args(['--ignore_nodeepscrub'])
        self.assertRaises(check_ceph_status.WarnError,
                          lambda: check_ceph_status.check_ceph_status(args))
