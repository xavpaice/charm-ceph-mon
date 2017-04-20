#!/usr/bin/python
#
# Copyright 2017 Canonical Ltd
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

import sys
from random import randint
import re
from time import sleep
from timeit import default_timer as timer

sys.path.append('lib')
sys.path.append('hooks/')

from charmhelpers.core.hookenv import action_get, log, action_fail

import ceph

"""
Given an OSD number, stepsize, and target weight, this script will attempt to
gradually reweight that OSD by stepsize at a time till it reaches the target,
waiting for the ceph health to return to HEALTH_OK or at least WARN but not
recovering before each operation.
"""


def wait_for_ceph(start_time, step_timeout):
    ceph_health_busy = ceph_busy()
    while ceph_health_busy:
        now = timer()
        if start_time + now > start_time + step_timeout:
            action_fail("Timed out, reached step timeout of {}".format(
                step_timeout))
        log("Waiting for ceph health before continuing")
        sleep(randint(5, 30))
        ceph_health_busy = ceph_busy()


def ceph_busy():
    ceph_health = ceph.get_ceph_health()
    log("ceph health {}".format(ceph_health['overall_status']))
    if ceph_health['overall_status'] == 'HEALTH_OK':
        return False
    if ceph_health['overall_status'] == 'HEALTH_ERR':
        log("ceph HEALTH_ERR, waiting for 10 secs for retry")
        sleep(10)
        ceph_health = ceph.get_ceph_health()
        log("ceph health {}".format(ceph_health['overall_status']))
        if ceph_health['overall_status'] == 'HEALTH_ERR':
            raise SystemError('Ceph health is HEALTH_ERR')
            action_fail("Ceph health is HEALTH_ERR, failing")
    if ceph_health['overall_status'] == 'HEALTH_WARN':
        ceph_pg_stat = ceph.get_ceph_pg_stat()
        for state in ceph_pg_stat['num_pg_by_state']:
            if re.match(r'.*backfill.*', state['name']):
                log("Found backfilling PGs")
                return True
            log("Ceph health is {} but not backfill, continue reweight".format(
                ceph_health['overall_status']))
            return False


def step_weight(osd_num, target_weight, stepsize, current_weight):
    if target_weight < current_weight:  # We want to reduce the weight
        stepsize = abs(stepsize) * -1
    new_weight = current_weight + stepsize
    # if the diff between current and target is less than the step
    if abs(current_weight - target_weight) < abs(stepsize):
        new_weight = target_weight
    if current_weight != target_weight:
        try:
            ceph.reweight_osd(str(osd_num), str(new_weight))
        except Exception as e:
            log(e)
            action_fail("OSD reweight failed with message: {}".format(
                e.message))


def reweight_osd_slowly():
    timeout = action_get("timeout")
    step_timeout = action_get("step-timeout")
    start_time = timer()
    osd_num = action_get("osd-num")
    osd_id = "osd." + str(osd_num)
    target_weight = float(action_get("target-weight"))
    stepsize = float(action_get("stepsize"))
    current_weight = round(ceph.get_osd_weight(osd_id), 3)
    # TODO grab this into a background process, with timeout and control
    while current_weight != target_weight:
        now = timer()
        if start_time + now > start_time + timeout:
            action_fail("Timed out, reached timeout of {}".format(timeout))
        step_time = timer()
        wait_for_ceph(step_time, step_timeout)
        step_weight(osd_num, target_weight, stepsize, current_weight)
        current_weight = round(ceph.get_osd_weight(osd_id), 3)
        sleep(5)  # allow ceph to actually do something before next check


if __name__ == '__main__':
    reweight_osd_slowly()
