#!/usr/bin/env python3
#
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

from subprocess import CalledProcessError
import sys

sys.path.append('hooks')

from charmhelpers.contrib.storage.linux.ceph import create_erasure_profile
from charmhelpers.core.hookenv import action_get, log, action_fail


def make_erasure_profile():
    name = action_get("name")
    plugin = action_get("plugin")
    failure_domain = action_get("failure-domain")

    # jerasure requires k+m
    # isa requires k+m
    # local requires k+m+l
    # shec requires k+m+c

    if plugin == "jerasure":
        k = action_get("data-chunks")
        m = action_get("coding-chunks")
        try:
            create_erasure_profile(service='admin',
                                   erasure_plugin_name=plugin,
                                   profile_name=name,
                                   data_chunks=k,
                                   coding_chunks=m,
                                   failure_domain=failure_domain)
        except CalledProcessError as e:
            log(e)
            action_fail("Create erasure profile failed with "
                        "message: {}".format(e.message))
    elif plugin == "isa":
        k = action_get("data-chunks")
        m = action_get("coding-chunks")
        try:
            create_erasure_profile(service='admin',
                                   erasure_plugin_name=plugin,
                                   profile_name=name,
                                   data_chunks=k,
                                   coding_chunks=m,
                                   failure_domain=failure_domain)
        except CalledProcessError as e:
            log(e)
            action_fail("Create erasure profile failed with "
                        "message: {}".format(e.message))
    elif plugin == "local":
        k = action_get("data-chunks")
        m = action_get("coding-chunks")
        l = action_get("locality-chunks")
        try:
            create_erasure_profile(service='admin',
                                   erasure_plugin_name=plugin,
                                   profile_name=name,
                                   data_chunks=k,
                                   coding_chunks=m,
                                   locality=l,
                                   failure_domain=failure_domain)
        except CalledProcessError as e:
            log(e)
            action_fail("Create erasure profile failed with "
                        "message: {}".format(e.message))
    elif plugin == "shec":
        k = action_get("data-chunks")
        m = action_get("coding-chunks")
        c = action_get("durability-estimator")
        try:
            create_erasure_profile(service='admin',
                                   erasure_plugin_name=plugin,
                                   profile_name=name,
                                   data_chunks=k,
                                   coding_chunks=m,
                                   durability_estimator=c,
                                   failure_domain=failure_domain)
        except CalledProcessError as e:
            log(e)
            action_fail("Create erasure profile failed with "
                        "message: {}".format(e.message))
    else:
        # Unknown erasure plugin
        action_fail("Unknown erasure-plugin type of {}. "
                    "Only jerasure, isa, local or shec is "
                    "allowed".format(plugin))


if __name__ == '__main__':
    make_erasure_profile()
