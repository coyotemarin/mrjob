# -*- coding: utf-8 -*-
# Copyright 2009-2019 Yelp and Contributors
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
from __future__ import division
import datetime
import logging
import os.path
import time

from mrjob.aws import _boto3_paginate


log = logging.getLogger(__name__)

# Affirm: may eventually want to merge this into mrjob/pool.py. Modules named
# queue.py can be especially confusing to urllib3, used by boto3, on Python 3

# We first check if the cluster has been created after 8 minutes, then another
# 4, then 2, then every 1 minute for a max of 20 minutes.
_MAX_CREATION_WAIT_TIME = 1200       # 20 minutes
_START_CREATION_CHECK_SECONDS = 480  # 8 minutes
_MIN_CREATION_CHECK_SECONDS = 60     # 1 minute


class SchedulingQueue(object):
    """This class is used when there are no running clusters in the desired
    pool. In this situation we wish to spin up a new cluster, but we do not
    want to spin up one for every process currently running. Therefore this
    class defines a light-weight scheduling queue. This queue is built in S3
    to prevent introducing any new infrastructure and only performs writes of
    new objects and list operations to achieve read-after-write semantics."""

    def __init__(self, s3_fs, s3_prefix, max_spool_length, runner_self):
        """
        :param s3_fs: File system to use when interacting with S3.
        :param s3_prefix: Where we store store all of our markers.
                          This must be unique to the target pool.
        :param max_spool_length: The maximum number of clusters that we allow
                                 to start up in parallel.
        :param runner_self: Composes runner instance so we can access cluster
                            creation/wait methods.
        """
        self.s3_fs = s3_fs
        self.s3_prefix = s3_prefix
        self.max_spool_length = max_spool_length
        self._runner = runner_self

    def _get_event_markers(self, event_type, curr_date):
        """Helper function to get event markers at a given date.

        Since it is possible we are on the cusp of an hour, we check the
        previous and next hour as well. Since cluster creation should never
        take more than 20 minutes we don't need to look any further.
        """
        date_range = [curr_date - datetime.timedelta(hours=1), curr_date,
                      curr_date + datetime.timedelta(hours=1)]
        events = []
        for date in date_range:
            marker_glob = date.strftime('{}_%Y-%m-%dT%H-*') \
                            .format(event_type)
            marker_path = os.path.join(self.s3_prefix, marker_glob)
            events += list(self.s3_fs.ls(marker_path))
        return sorted(events)

    def _get_recent_attempt(self, creation_events, attempt_events):
        """Helper function to find all attempt events that fall after the
        last creation event."""
        last_creation = creation_events[-1].split('_')[1]
        old_attempts_indices = [i for i, marker in enumerate(attempt_events)
                                if marker.split('_')[1] <= last_creation]
        assert len(old_attempts_indices) > 0, 'The list can not be empty here'
        return attempt_events[old_attempts_indices[-1] + 1:]

    def _recheck_clusters(self):
        emr_client = self._runner.make_emr_client()
        req_pool_hash = self._runner._pool_hash()
        for cluster_summary in _boto3_paginate(
                'Clusters', emr_client, 'list_clusters',
                ClusterStates=('RUNNING', 'WAITING')):
            _id = cluster_summary['Id']
            cluster = self._runner.cluster_cache.describe_cluster(_id)
            if self._runner._compare_cluster_setup(emr_client, cluster,
                                                   req_pool_hash) is not None:
                return True
        return False

    def enter_cluster_creation_queue(self):
        """The scheduling queue entry point."""

        # This date is our uniquetimestamp, we refer to it at all times
        curr_date = datetime.datetime.now()

        # Put a marker that we are trying to start a cluster. We maintain two
        # event types: "attempts_" for each process trying to create a cluster
        # and "creation_" after a process proceeds to create a cluster
        curr_date_str = curr_date.strftime('%Y-%m-%dT%H-%M%S%f')
        attempt_marker = os.path.join(self.s3_prefix,
                                      'attempt_{}'.format(curr_date_str))
        creation_marker = os.path.join(self.s3_prefix,
                                       'creation_{}'.format(curr_date_str))
        self.s3_fs.danger_touchz(attempt_marker)

        # Now we wish to find out who else is here and trying to schedule
        attempt_events = self._get_event_markers('attempt', curr_date)
        creation_events = self._get_event_markers('creation', curr_date)

        assert len(creation_events) <= len(attempt_events), \
            'Something is wrong'

        old_creation_events = set()
        if len(creation_events) != 0:
            # This case is quite subtle. Since we should only have run this
            # code when there were no active clusters, if there is already a
            # creation event in the pool then either
            #   1) between the initial list cluster and this point a cluster
            #      finished creation
            #   2) a cluster had been created but went down for some reason
            # Therefore to handle the first case  we list the clusters and
            # return if we find anything. If there are in fact no clusters
            # we proceed as normal but ignore these old creation events along
            # with attempt events before the last creation event.
            log.info('Found odd creation event, performing extra checks.')
            # For some reason the below check is returning valid clusters
            # while the outer loop is unable to find any. This is really odd
            # and it's unclear why it's happening.
            # if self._recheck_clusters():
            #     log.info('Rechecked clusters and found a valid cluster.')
            #     return
            attempt_events = self._get_recent_attempt(creation_events,
                                                      attempt_events)
            old_creation_events = set(creation_events)

        # At this point we let the first max_spool_length` processes create
        # clusters. It is fine if more clusters attempt after this since their
        # markers will be at the end of the list
        if attempt_marker in attempt_events[0:self.max_spool_length]:
            log.info('Grabbed cluster creation lock; creating cluster')
            self._runner._cluster_id = self._runner._create_cluster()
            self._runner._wait_for_cluster()
            self.s3_fs.danger_touchz(creation_marker)
            return True

        # Some other process or processes are creating clusters so wait
        # for any one of them to finish and write it's marker. There is
        # no need to wait for all of them as we will re-enter the find
        # cluster logic after this.
        curr_sleep_time = _START_CREATION_CHECK_SECONDS
        total_curr_wait = 0
        while len(creation_events) == 0:
            log.info('Unable to grab cluster creation lock; sleeping for'
                     ' {}s'.format(curr_sleep_time))
            time.sleep(curr_sleep_time)
            total_curr_wait += curr_sleep_time
            all_creation_events = set(self._get_event_markers('creation',
                                                              curr_date))
            creation_events = all_creation_events - old_creation_events
            curr_sleep_time = max(curr_sleep_time // 2,
                                  _MIN_CREATION_CHECK_SECONDS)
            if total_curr_wait >= _MAX_CREATION_WAIT_TIME:
                log.info('Waited max time of {}s; re-entering cluster'
                         ' discovery loop'.format(total_curr_wait))
                return False
        log.info('Saw creation events ({}), exiting waiting loop'
                 .format(', '.join(creation_events)))
        return True
