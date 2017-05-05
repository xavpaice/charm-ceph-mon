#!/usr/bin/env python

# Copyright (C) 2014 Canonical
# All Rights Reserved
# Authors: Jacek Nykis <jacek.nykis@canonical.com>
#          Xav Paice <xav.paice@canonical.com>

import re
import argparse
import json
import subprocess
import nagios_plugin


def check_ceph_status(args):
    regex = r'\d+ pgs (?:backfill_wait|backfilling|degraded|recovery_wait|' \
            'stuck unclean)|recovery \d+\/\d+ objects (?:degraded|misplaced)'
    status_critical = False
    if args.status_file:
        nagios_plugin.check_file_freshness(args.status_file, 3600)
        status_data = json.loads(open(args.status_file).read())
    else:
        try:
            tree = subprocess.check_output(['ceph',
                                            'status',
                                            '--format json'])
        except subprocess.CalledProcessError as e:
            raise nagios_plugin.UnknownError(
                "UNKNOWN: ceph status command failed with error: {}".format(e))
        status_data = json.loads(tree)

    if ('health' not in status_data.keys() or
            'monmap' not in status_data.keys() or
            'pgmap' not in status_data.keys() or
            'osdmap'not in status_data.keys()):
        raise nagios_plugin.UnknownError('UNKNOWN: status data is incomplete')

    if status_data['health']['overall_status'] != 'HEALTH_OK':
        # Health is not OK, check if any lines are not in our list of OK
        # any lines that don't match, check is critical
        status_msg = []
        for status in status_data['health']['summary']:
            if not re.match(regex, status['summary']):
                status_critical = True
                status_msg.append(status['summary'])
        # If we got this far, then the status is not OK but the status lines
        # are all in our list of things we consider to be operational tasks.
        # Check the thresholds and return OK if within boundary.
        try:
            if status_data['pgmap']['degraded_ratio'] > args.degraded_thresh:
                status_critical = True
                status_msg.append("Degraded ratio: {}%".format(
                    status_data['pgmap']['degraded_ratio'] * 100))
        except KeyError:
            pass
        try:
            if status_data['pgmap']['misplaced_ratio'] > args.misplaced_thresh:
                status_critical = True
                status_msg.append("Misplaced ratio: {}%".format(
                    status_data['pgmap']['misplaced_ratio'] * 100))
        except KeyError:
            pass
        if status_critical:
            msg = 'CRITICAL: ceph health: "{} {}"'.format(
                  status_data['health']['overall_status'],
                  ", ".join(status_msg))
            raise nagios_plugin.CriticalError(msg)
        if status_data['health']['overall_status'] == 'HEALTH_WARN':
            msg = "WARNING: ceph misplaced {}%, degraded {}%".format(
                  status_data['pgmap']['misplaced_ratio'] * 100,
                  status_data['pgmap']['degraded_ratio'] * 100)
            raise nagios_plugin.WarnError(msg)
    print("All OK")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check ceph status')
    parser.add_argument('-f', '--file', dest='status_file',
                        default=False,
                        help='Optional file with "ceph status" output')
    parser.add_argument('--degraded_thresh', dest='degraded_thresh',
                        default=0.1,
                        help="Threshold for degraded ratio (0.1 = 10%)")
    parser.add_argument('--misplaced_thresh', dest='misplaced_thresh',
                        default=0.1,
                        help="Threshold for misplaced ratio (0.1 = 10%)")
    args = parser.parse_args()
    nagios_plugin.try_check(check_ceph_status, args)
