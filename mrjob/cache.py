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
from contextlib import contextmanager
from datetime import datetime
import fcntl
import json
import logging
import os

from mrjob.aws import _boto3_paginate

log = logging.getLogger(__name__)

# This defines how often to re-list clusters.
_DEFAULT_CACHE_LIST_FREQUENCY_SECONDS = 30


class ClusterCache(object):
    """Read-through file cache of cluster info to reduce EMR API calls.

    Note: we still hit the EMR API in
        - :py:meth:`~mrjob.emr.EMRJobRunner._wait_for_cluster`
          (waiting for a cluster to spin up)
        - :py:meth:`~mrjob.emr.EMRJobRunner._get_cluster_info`
          (creates in-memory "cache" of cluster info)
    """
    def __init__(self, emr_client, cache_filepath, cache_file_ttl,
                 list_frequency=_DEFAULT_CACHE_LIST_FREQUENCY_SECONDS):
        """Init cluster cache.

        :param emr_client: boto3 EMR client to use for list and describe calls
        :param cache_filepath: absolute local path to the cluster cache
        :param cache_file_ttl: expire (truncate) the cache after this many days
        :param list_frequency: list at most only every this many seconds
        """
        self._emr_client = emr_client
        self._cache_filepath = cache_filepath
        self._cache_file_ttl = cache_file_ttl
        self._list_frequency = list_frequency

    @staticmethod
    def setup(cache_filepath):
        if not os.path.isfile(cache_filepath):
            open(cache_filepath, 'a').close()
            open(cache_filepath + '.age_marker', 'a').close()
            open(cache_filepath + '.list_marker', 'a').close()

    @contextmanager
    def cache_mutex(self, mode):
        try:
            self._fd = open(self._cache_filepath, mode=mode)
            fcntl.flock(self._fd, fcntl.LOCK_EX)
            yield self._fd
        finally:
            # We must always ensure we flush before we unlock and close, lest
            # we unlock and another process begins reading and writing before
            # this process has had a change to write its buffer.
            self._fd.flush()
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            self._fd.close()
            self._fd = None

    def _load_cache(self, fd):
        if os.stat(self._cache_filepath).st_size == 0:
            return {}
        else:
            return json.load(fd)

    def _dump_cache(self, content, fd, truncate):
        # We must seek back to the beginning of the file before writing.
        fd.seek(0)
        if truncate:
            fd.truncate()
        json.dump(content, fd, default=str)

    def _get_list_paginator(self, states):
        return _boto3_paginate('Clusters', self._emr_client,
                               'list_clusters', ClusterStates=states)

    def _emr_cluster_describe(self, cluster_id):
        """Describe the cluster and get the instance group/fleet info."""
        cluster_info = self._emr_client.describe_cluster(
                                ClusterId=cluster_id)['Cluster']

        collect_type = cluster_info.get('InstanceCollectionType', None)
        if collect_type == 'INSTANCE_FLEET':
            instance_info = list(_boto3_paginate(
                'InstanceFleets', self._emr_client, 'list_instance_fleets',
                ClusterId=cluster_id))
        else:
            instance_info = list(_boto3_paginate(
                'InstanceGroups', self._emr_client, 'list_instance_groups',
                ClusterId=cluster_id))

        return {
            'Cluster': cluster_info,
            'Instances': instance_info
        }

    def _handle_cache_expiry(self):
        age_marker_file = self._cache_filepath + '.age_marker'
        mtime = os.stat(age_marker_file).st_mtime
        days_old = (datetime.utcnow() - datetime.utcfromtimestamp(mtime)).days
        if days_old > self._cache_file_ttl:
            log.info('Cluster cache expired, truncating cache')
            open(age_marker_file, 'w').close()  # update mtime
            open(self._cache_filepath, 'w').close()  # truncate cache

    def _should_list_and_populate(self):
        list_marker_file = self._cache_filepath + '.list_marker'
        mtime = os.stat(list_marker_file).st_mtime
        seconds_old = (datetime.utcnow() -
                       datetime.utcfromtimestamp(mtime)).seconds
        if seconds_old > self._list_frequency:
            open(list_marker_file, 'w').close()  # update mtime
            return True
        return False

    def describe_cluster(self, cluster_id):
        """Describes an EMR cluster from the given ID. Also describes and
        caches the instance group/fleet info.

        If cluster info for this ID exists in the cache return this data;
        otherwise make an EMR API call to retry this data. Since mrjob clusters
        should never be modified, this cached data will always be valid except
        cluster state.
        """
        # If there is no cluster cache file then fallback to a normal describe.
        if self._cache_filepath is None:
            return self._emr_cluster_describe(cluster_id)

        with self.cache_mutex('r+') as fd:
            self._handle_cache_expiry()

            content = self._load_cache(fd)

            # Check if the cluster id is present
            cluster = content.get(cluster_id, None)
            if cluster:
                log.debug('Cluster cache hit: found cluster {}'
                          .format(cluster_id))
                return cluster
            log.debug('Cluster cache miss: no entry for cluster {}'
                      .format(cluster_id))

            # If there is no cluster with this id we get the info from EMR
            content[cluster_id] = self._emr_cluster_describe(cluster_id)

            # There is no reason to truncate as the content will never shorten
            self._dump_cache(content, fd, False)

            return content[cluster_id]

    def list_clusters_and_populate_cache(self, states, force_relist=False):
        """Lists EMR clusters with specified state and populates the cache
        with their info.

        Only lists the clusters if the list marker file is older than the list
        frequency. Only describes the cluster info if the ID does not exist in
        the cache.

        Additionally, we only put entries in the cache if their state matches
        the specified state and update the state if it had changed. Therefore
        this function may remove entries from the cache.
        """
        assert self._cache_filepath is not None, \
            'This code requires the cluster cache to be present'

        with self.cache_mutex('r+') as fd:

            content = self._load_cache(fd)

            # Check if we are updating the cache; if not just return the
            # current cache contents. Always list if the cache is empty.
            if content and not (self._should_list_and_populate() or
                                force_relist):
                return content

            # List all clusters with a valid state. For each of these clusters
            # check if it is in the cache. If not describe it and add it to the
            # cache. Otherwise ensure the state is up-to-date.
            new_content = {}
            for cluster_summary in self._get_list_paginator(states):
                cluster_id = cluster_summary['Id']
                cluster_info = content.get(cluster_id, None)
                if cluster_info is None:
                    cluster_info = self._emr_cluster_describe(cluster_id)
                else:
                    cluster_info['Cluster']['Status']['State'] = \
                        cluster_summary['Status']['State']
                new_content[cluster_id] = cluster_info

            # We must truncate as the content may have shortened.
            self._dump_cache(new_content, fd, True)

            return new_content
