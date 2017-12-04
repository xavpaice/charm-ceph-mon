# Copyright 2016 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from mock import mock
import sys

from test_utils import CharmTestCase

# python-apt is not installed as part of test-requirements but is imported by
# some charmhelpers modules so create a fake import.
mock_apt = mock.MagicMock()
sys.modules['apt'] = mock_apt
mock_apt.apt_pkg = mock.MagicMock()

# mocking for rados
mock_rados = mock.MagicMock()
sys.modules['rados'] = mock_rados
mock_rados.connect = mock.MagicMock()

# mocking for psutil
mock_psutil = mock.MagicMock()
sys.modules['psutil'] = mock_psutil
mock_psutil.disks = mock.MagicMock()

with mock.patch('charmhelpers.contrib.hardening.harden.harden') as mock_dec:
    mock_dec.side_effect = (lambda *dargs, **dkwargs: lambda f:
                            lambda *args, **kwargs: f(*args, **kwargs))
    # import health actions as actions
    import ceph_ops as actions


class OpsTestCase(CharmTestCase):

    def setUp(self):
        super(OpsTestCase, self).setUp(
            actions, ["check_output",
                      "action_get",
                      "action_fail",
                      "open"])

    def test_get_health(self):
        actions.get_health()
        cmd = ['ceph', 'health']
        self.check_output.assert_called_once_with(cmd)
