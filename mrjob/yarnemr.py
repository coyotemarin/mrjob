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
import json
import logging
import operator
import os
import re
import time
from datetime import datetime
from datetime import timedelta

from mrjob.compat import version_gte
from mrjob.emr import _attempt_to_acquire_lock
from mrjob.emr import _EMR_STATES_STARTING
from mrjob.emr import _EMR_STATUS_RUNNING
from mrjob.emr import _EMR_STATUS_TERMINATING
from mrjob.emr import _make_lock_uri
from mrjob.emr import _POOLING_SLEEP_INTERVAL
from mrjob.emr import EMRJobRunner
from mrjob.pool import _pool_hash_and_name
from mrjob.py2 import urljoin
from mrjob.step import _is_spark_step_type
from mrjob.step import StepFailedException

log = logging.getLogger(__name__)


# Amount of time in seconds before we timeout yarn api calls.
_YARN_API_TIMEOUT = 20

# which port to connect to the YARN resource manager on
_YARN_RESOURCE_MANAGER_PORT = 8088

# base path for YARN resource manager
_YRM_BASE_PATH = '/ws/v1/cluster'

# We lock scheduling on an entire cluster while scheduling a job on it. This
# is unlike the EMR cluster which locks at a per-step granularity. As such,
# there is a risk of starvation if a running process holding a lock is killed.
# Therefore, rather than releasing the lock we simply timeout the lock after
# 10 seconds. Since the lock is only needed to reduce scheduling contention
# this method is sufficient.
_LOCK_TIMEOUT_SECS = 10

# The amount of time to wait before re-querying clusters while there are still
# valid clusters remaining (unlike :py:data:mrjob.emr._POOLING_SLEEP_INTERVAL
# which is the time to wait when there are no valid clusters remaining).
_POOLING_JITTER = 3

# Do not schedule on a cluster if there are more than this many yarn apps
# pending or running, respectively.
_MAX_APPS_PENDING = 2
_MAX_APPS_RUNNING = 40

# Approximate maximum time in seconds to wait while a new cluster is starting
# and bootstrapping. This is approximate since we only wait to the closest
# multiple of the option `check_cluster_every` rounded down.
_NEW_CLUSTER_WAIT_MINS = 20

# Re-enter the find cluster loop at most this many times after we are unable to
# find / create a cluster. Once this limit is exceed we raise an exception.
_MAX_FIND_RETRY_COUNT_FOR_POOL_CAP = 3


class _ResourceConstraint(object):
    """Abstracts a constraint on a resource. Checks whether a metric value
    hold under an operator and threshold."""

    def __init__(self, operation, metrics_key, threshold):
        self.operation = operation
        self.metrics_key = metrics_key
        self.threshold = threshold

    def _failure_callback(self, current_metric):
        """Called when a metric constraint is not satisfied"""
        log.info('    metric "%s" did not satisfy %s(%d,%d)',
                 self.metrics_key, self.operation.__name__,
                 current_metric, self.threshold)

    def __call__(self, metric_dict):
        result = self.operation(metric_dict[self.metrics_key], self.threshold)
        if not result:
            self._failure_callback(metric_dict[self.metrics_key])
        return result


class YarnEMRJobRunner(EMRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on an EMR cluster by  running
    directly on YARN. Invoked when you run your job with ``-r yarnemr``.
    """
    alias = 'yarnemr'

    OPT_NAMES = EMRJobRunner.OPT_NAMES | {
        'expected_cores',
        'expected_memory',
        'max_pool_cluster_count',
        'yarn_logs_output_base'
    }

    def _required_arg(self, name):
        if self._opts[name] is None:
            raise ValueError('The argument "{}" is required for the YARN'
                             ' EMR runner'.format(name))

    def __init__(self, **kwargs):
        """:py:class:`~mrjob.yarnemr.YarnEMRJobRunner` takes the same arguments
        as :py:class:`~mrjob.emr.EMRJobRunner` with just one additional param,
        `expected_memory`."""
        super(YarnEMRJobRunner, self).__init__(**kwargs)

        self._required_arg('expected_cores')
        self._required_arg('expected_memory')
        self._required_arg('yarn_logs_output_base')
        self._required_arg('ec2_key_pair_file')

        self._ensure_fair_scheduler()

    def _ensure_fair_scheduler(self):
        """Ensures the yarn-site section of the emr_configurations option
        contains the flag to use the YARN fair scheduler."""
        scheduler_key = 'yarn.resourcemanager.scheduler.class'
        fair_scheduler = 'org.apache.hadoop.yarn.server.resourcemanager.' \
                         'scheduler.fair.FairScheduler'
        found_yarn_config = False
        if self._opts['emr_configurations'] is None:
            self._opts['emr_configurations'] = []
        for conf in self._opts['emr_configurations']:
            if conf['Classification'] == 'yarn-site':
                found_yarn_config = True
                log.debug('Found yarn config, setting yarn scheduler to'
                          ' use fair scheduler.')
                if scheduler_key in conf['Properties']:
                    if conf['Properties'][scheduler_key] != fair_scheduler:
                        log.error('Not using yarn fair scheduler.')
                    else:
                        log.debug('Found yarn config, already using fair'
                                  ' scheduler')
                    break
                conf['Properties'][scheduler_key] = fair_scheduler
        if not found_yarn_config:
            log.debug('Did not find yarn config, creating yarn scheduler with'
                      ' entry to use fair scheduler.')
            self._opts['emr_configurations'].append({
                'Classification': 'yarn-site',
                'Properties': {scheduler_key: fair_scheduler}
            })

    def _create_and_wait_for_cluster(self):
        self._cluster_id = self._create_cluster()
        self._created_cluster = True
        self._wait_for_cluster()

    def _launch_yarn_emr_job(self):
        """Create an empty cluster on EMR if needed, sets self._cluster_id to
        the cluster's ID, and submits the job to the cluster via ssh.

        This logic/code is intended to mirror
        :py:meth:`~mrjob.emr.EMRJobRunner._launch_emr_job`; however this is
        intentionally WET as the logic has inevitably started to drift. The
        function was renamed to `_launch_yarnemr_job` to avoid confusion.
        """
        # Resource constraints for if we try to find a cluster
        resource_constraints = [
            _ResourceConstraint(operator.ge, 'availableVirtualCores',
                                self._opts['expected_cores']),
            _ResourceConstraint(operator.ge, 'availableMB',
                                self._opts['expected_memory']),
            _ResourceConstraint(operator.lt, 'appsPending',
                                _MAX_APPS_PENDING),
            _ResourceConstraint(operator.lt, 'appsRunning',
                                _MAX_APPS_RUNNING)
        ]

        # Try to find a cluster (this is pretty much a while true loop since
        # we break whenever we get a cluster id anyways).
        attempt_count = 0
        while not self._cluster_id:
            attempt_count += 1

            # If we are not pooling clusters then just create a new cluster
            if not self._opts['pool_clusters']:
                self._create_and_wait_for_cluster()
                break

            # Try to find a cluster, break if we find a valid cluster
            find_cluster_ret = self._find_cluster(resource_constraints)
            if find_cluster_ret:
                self._cluster_id = find_cluster_ret
                break

            # If there is no max cluster count then create a cluster
            if not self._opts['max_pool_cluster_count']:
                self._create_and_wait_for_cluster()
                break

            # Max cluster limiting logic:

            # Each time we enter this check force a re-list to ensure
            # we are as up-to-date as possible.
            cluster_up_states = _EMR_STATES_STARTING + _EMR_STATUS_RUNNING
            cluster_info_list = self.cluster_cache.\
                list_clusters_and_populate_cache(cluster_up_states, True)
            pool_name_hash = (self._pool_hash(), self._opts['pool_name'],)
            valid_cluster_count = sum(
                pool_name_hash == _pool_hash_and_name(c['Cluster'])
                for _, c in cluster_info_list.items()
            )

            if valid_cluster_count >= self._opts['max_pool_cluster_count']:
                log.info('Cluster pool %s full, %d out of %d clusters',
                         self._opts['pool_name'], valid_cluster_count,
                         self._opts['max_pool_cluster_count'])

            # Ideally if we are under the max pool cap then create a new.
            # However is it possible other processes are at this state so we
            # must lock on this cluster creation. We do so by constructing a
            # lock unique to this pool and the number of valid clusters plus
            # an attempt index. Note, there is another issue: it is possible
            # a cluster goes down and we will have to re-create a cluster with
            # this same number, so these locks need to expire at some point.
            # The new cluster wait time is a reasonable expiry.
            status = False
            for i in range(valid_cluster_count + 1,
                           self._opts['max_pool_cluster_count'] + 1):
                log.info('Attempting to create cluster #%d', i)
                lock_uri = self._creation_lock_uri('{}_{}'
                                                   .format(*pool_name_hash), i)
                status = _attempt_to_acquire_lock(
                            self.fs.s3, lock_uri,
                            self._opts['cloud_fs_sync_secs'],
                            self._job_key,
                            mins_to_expiration=_NEW_CLUSTER_WAIT_MINS)
                if status:
                    break
            if status:
                log.info('Grabbed lock, creating cluster #%d', i)
                self._create_and_wait_for_cluster()
                break

            # If this is the last attempt then we fail the job run
            if attempt_count == _MAX_FIND_RETRY_COUNT_FOR_POOL_CAP:
                desc = 'Unable to find cluster within limit after 3 attempts.'
                reason = ('Current cluster count ({}) >= max cluster'
                          ' count ({})'
                          .format(valid_cluster_count,
                                  self._opts['max_pool_cluster_count']))
                raise StepFailedException(step_desc=desc, reason=reason)
        # </end of loop>

        if not self._created_cluster:
            log.info('Adding our job to existing cluster %s (%s)' %
                     (self._cluster_id, self._address_of_master()))

        # now that we know which cluster it is, check for Spark support
        if self._has_spark_steps():
            self._check_cluster_spark_support()

        if version_gte(self.get_image_version(), '4.3.0'):
            self.fs.ssh.use_sudo_over_ssh()

        self._execute_job()

    def _launch(self):
        """Set up files and then launch our job on the EMR cluster."""
        self._prepare_for_launch()
        self._launch_yarn_emr_job()

    def _scheduling_lock_uri(self, cluster_id):
        """Lock unique per cluster"""
        return _make_lock_uri(self._opts['cloud_tmp_dir'], cluster_id, 'yarn')

    def _creation_lock_uri(self, pool_name_hash, cluster_num):
        """Lock unique per pool (name and hash) per cluster number"""
        return _make_lock_uri(self._opts['cloud_tmp_dir'],
                              pool_name_hash, cluster_num)

    def _check_cluster_state(self, cluster, resource_constraints):
        """Queries the cluster for basic metrics and checks these against
        a list of constraints.

        :return: The number of available mb on the cluster if the cluster
                 satisfies the constraints and -1 otherwise.
        """
        host = cluster['MasterPublicDnsName']
        try:
            metrics = self._yrm_get('metrics', host=host)['clusterMetrics']
        except IOError as ex:
            log.info('    error while querying cluster metrics: {}'.format(
                str(ex)))
            return -1

        log.debug('Cluster metrics: {}'.format(metrics))
        if all(constraint(metrics) for constraint in resource_constraints):
            log.debug('    OK - valid cluster state')
            return metrics['availableMB']
        else:
            log.debug('    failed resource constraints')
            return -1

    def _usable_clusters(self, valid_clusters, invalid_clusters,
                         locked_clusters, resource_constraints):
        """Get clusters that this runner can join.

        Note: this will update pass-by-reference arguments `valid_clusters`
        and `invalid_clusters`.

        Also note that we cache aggressively here. All cluster information is
        cached, and moreover, we only re-list clusters every 30 seconds. It is
        worth discussing the situation when a cluster is created and when one
        is terminated. The former is not a problem since the pool wait loop
        will eventually find the cluster. There is an issue towards the end of
        the pool wait time but this is not a frequent situation, and would only
        result in an extra cluster. The latter case of terminated clusters is
        subtler and trickier. Luckily when we list clusters we get their state.
        Therefore we only put entries back into the cache if they have a
        waiting/running state. This means that a terminated cluster can only be
        in the cache up to our frequency check time (30 seconds). In this case,
        in the yarn runner, querying the yarn api will fail and skip over the
        cluster. Therefore this situation should be ok.

        :param valid_clusters: A map of cluster id to cluster info with a valid
                               setup; thus we do not need to check their setup
                               again.
        :param invalid_clusters: A set of clusters with an invalid setup; thus
                                 we skip these clusters.
        :param locked_clusters: A set of clusters managed by the callee that
                                are in a "locked" state.
        :param resource_constraints: A list of various constraints on the
                                     resources of the target cluster.

        :return: list of cluster ids sorted by available memory in
                 descending order
        """
        emr_client = self.make_emr_client()
        req_pool_hash = self._pool_hash()

        # list of (available_mb, cluster_id)
        cluster_list = []

        cluster_info_list = self.cluster_cache.\
            list_clusters_and_populate_cache(_EMR_STATUS_RUNNING)
        for cluster_id, cluster_info in cluster_info_list.items():

            # ignore this cluster if the setup is invalid
            if cluster_id in invalid_clusters:
                log.debug('    excluded')
                continue

            # only if we haven't seen this cluster before we check the setup
            if cluster_id not in valid_clusters:
                if not self._compare_cluster_setup(
                        emr_client, cluster_info, req_pool_hash):
                    invalid_clusters.add(cluster_id)
                    continue
                valid_clusters[cluster_id] = cluster_info

            # this may be a retry due to locked clusters; so skip it
            if cluster_id in locked_clusters:
                log.debug('    excluded')
                continue

            # always check the state
            available_md = self._check_cluster_state(cluster_info['Cluster'],
                                                     resource_constraints)
            if available_md == -1:
                # don't add to invalid cluster list since the cluster may
                # be valid when we next check
                continue

            cluster_list.append((available_md, cluster_id,))

        return [_id for _, _id in sorted(cluster_list, reverse=True)]

    def _find_cluster(self, resource_constraints):
        """Find a cluster that can host this runner. Prefer clusters with
        more memory.

        Return a cluster id if a cluster is found, otherwise None.
        """
        valid_clusters = {}
        invalid_clusters = set()
        locked_clusters = []

        max_wait_time = self._opts['pool_wait_minutes']
        bypass_pool_wait = self._opts['bypass_pool_wait']

        now = datetime.now()
        end_time = now + timedelta(minutes=max_wait_time)

        log.info('Attempting to find an available cluster...')
        while now <= end_time:
            # remove any cluster from the locked list if it has been there
            # for more than _LOCK_TIMEOUT_SECS
            locked_clusters = [
                (c, t) for (c, t) in locked_clusters
                if now - t < timedelta(seconds=_LOCK_TIMEOUT_SECS)
            ]
            target_cluster_list = self._usable_clusters(
                valid_clusters, invalid_clusters,
                {l[0] for l in locked_clusters}, resource_constraints)
            log.debug('  Found %d usable clusters%s%s',
                      len(target_cluster_list),
                      ': ' if target_cluster_list else '',
                      ', '.join(c for c in target_cluster_list))

            if target_cluster_list:
                for cluster_id in target_cluster_list:
                    status = _attempt_to_acquire_lock(
                        self.fs.s3, self._scheduling_lock_uri(cluster_id),
                        self._opts['cloud_fs_sync_secs'], self._job_key,
                        mins_to_expiration=(_LOCK_TIMEOUT_SECS / 60.0))
                    if status:
                        log.info('Acquired lock on cluster %s', cluster_id)
                        return cluster_id
                    else:
                        locked_clusters.append((cluster_id, now,))
                log.info("Cannot acquire any of the %d cluster scheduling"
                         " locks", len(locked_clusters))
                # sleep for a couple seconds before trying again
                time.sleep(_POOLING_JITTER)
                now += timedelta(seconds=_POOLING_JITTER)
            elif bypass_pool_wait and not valid_clusters:
                # Enter cluster creation queue. Once we exit, do nothing so we
                # just re-enter the _find_cluster loop.
                self.scheduling_queue.enter_cluster_creation_queue()
            elif max_wait_time == 0:
                return None
            else:
                log.info('No clusters available in pool %s. Checking again in'
                         ' %d seconds...', self._opts['pool_name'],
                         _POOLING_SLEEP_INTERVAL)
                time.sleep(_POOLING_SLEEP_INTERVAL)
                now += timedelta(seconds=_POOLING_SLEEP_INTERVAL)

        return None

    def _wait_for_cluster(self):
        """Since we are not submitting EMR steps (which will wait to run
        by default) we must manually wait till the cluster has finishing
        bootstrapping before proceeding.

        We sadly write our own waiter since 1) botocore's waiters do not work
        with our retry logic and 2) they do not support intermediate logging.
        We closely mirror the logic in :py:meth:`botocore.waiter.Waiter.wait`.
        """
        emr_client = self.make_emr_client()
        cluster_id = self._cluster_id

        sleep_amount = self._opts['check_cluster_every']
        max_attempts = _NEW_CLUSTER_WAIT_MINS * 60 // sleep_amount
        num_attempts = 0

        while True:
            num_attempts += 1
            time.sleep(sleep_amount)
            log.info('Checking cluster %s for running state, attempt %d of'
                     ' %d', cluster_id, num_attempts, max_attempts)
            response = emr_client.describe_cluster(
                ClusterId=cluster_id
            )
            state = response['Cluster']['Status']['State']
            if state in _EMR_STATUS_RUNNING:
                master_ip = response['Cluster']['MasterPublicDnsName']
                log.info('Waiting complete, cluster %s reached running'
                         ' state %s (%s)', cluster_id, state, master_ip)
                return
            elif state in _EMR_STATUS_TERMINATING:
                raise Exception('Waiting failed, cluster {} reached error'
                                ' state {}'.format(cluster_id, state))
            else:
                assert state in _EMR_STATES_STARTING, 'Reached invalid state'
            if num_attempts >= max_attempts:
                raise Exception('Max attempts exceeded, cluster {} in'
                                ' state {}'.format(cluster_id, state))

    def _write_to_log_file(self, filename, text):
        """Simply write string to the output dir."""
        file_path = os.path.join(self._opts['yarn_logs_output_base'],
                                 '{}_{}'.format(self._job_key, filename))
        with open(file_path, 'w') as fp:
            fp.write(text)
        return file_path

    def _run_ssh_on_master(self, cmd_args, desc, stdin=None, log_file=None):
        """"Wraps :py:meth:`~mrjob.fs.ssh.SSHFilesystem._ssh_run` with our
        exception logic.

        The ``desc`` param to be used in info and error logging. If `log_file``
        is specified log a failure to the file rather than stdout.
        """

        host = self._address_of_master()
        try:
            log.info('Running %s command over ssh', desc)
            stdout, stderr = self.fs.ssh._ssh_run(host, cmd_args, stdin=stdin)
            return stdout, stderr
        except IOError as ex:
            if log_file:
                file_path = self._write_to_log_file(log_file, str(ex))
                log.info('  A failure occured; logging stderr to %s',
                         file_path)
            else:
                log.info('  A failure occured; printing stderr logs to stdout')
                log.info(str(ex))
            raise StepFailedException(step_desc=desc.capitalize(),
                                      reason='see above logs')

    def _special_yarn_args_formatting(self, command_args):
        """We need to do a couple things to the command arguments we get back
        from the bin runner.

        1) Add in spark config to cause command line call to return immediately

        2) Find any cmdenvs references in the command and wrap the arg in
        single quotes to prevent bash from interpolated the variable when
        we run via ssh.
        """
        app_wait_conf = 'spark.yarn.submit.waitAppCompletion'
        if any(arg.startswith(app_wait_conf) for arg in command_args):
            raise ValueError('User should not be setting spark'
                             ' option {}'.format(app_wait_conf))
        if '--conf' in command_args:
            first_conf_index = command_args.index('--conf')
        else:
            first_conf_index = command_args.index('--spark')
        command_args.insert(first_conf_index, '--conf')
        command_args.insert(first_conf_index+1,
                            '{}=false'.format(app_wait_conf))

        cmdenv_var = ['${}'.format(key) for key in self._opts['cmdenv'].keys()]
        for idx, arg in enumerate(command_args):
            if any(var in arg for var in cmdenv_var):
                command_args[idx] = "'{}'".format(arg)

        return command_args

    def _build_step(self, step_num):
        """Borrow logic from :py:meth:`~mrjob.emr.EMRJobRunner._launch_emr_job`
        with modifications to handle spark-only support.
        """
        step = self._get_step(step_num)

        if _is_spark_step_type(step['type']):
            method = self._spark_step_hadoop_jar_step
        elif step['type'] == 'streaming':
            raise AssertionError('Hadoop Streaming not yet supported')
        elif step['type'] == 'jar':
            raise AssertionError('Jar steps not yet supported')
        else:
            raise AssertionError('Bad step type: %r' % (step['type'],))

        command_args = method(step_num)['Args']
        assert command_args[0] == 'spark-submit', 'Programmatic error'
        return self._special_yarn_args_formatting(command_args)

    def _execute_job(self):
        """Runs master setup if needed and submits spark job."""

        # master setup
        if self._master_node_setup_mgr.paths() or self._opts['master_setup']:
            mns_stdin = open(self._master_node_setup_script_path)
            self._run_ssh_on_master(['bash -s'], 'master setup',
                                    stdin=mns_stdin, log_file='ms.sh')
        else:
            log.debug('No master setup to run')

        # spark submit
        if self._num_steps() > 1:
            raise AssertionError('Multiple steps not yet supported in'
                                 ' yarn runner')
        spark_submit_command = self._build_step(0)
        _, stderr = self._run_ssh_on_master(spark_submit_command,
                                            'spark submit')

        # The application ID appears in several places in these logs, but we
        # choose to take it from the log-line where we submit the application
        # to the YARN resource manager
        app_search = re.search(b'Submitting application (application[0-9_]*)'
                               b' to ResourceManager', stderr)
        self._appid = app_search.groups()[0].decode('utf-8')
        log.info('Application successfully submitted with application id'
                 ' %s', self._appid)

    def _get_application_info(self):
        """Queries the cluster for state of application."""
        return self._yrm_get('apps/{}'.format(self._appid))['app']

    def _get_yarn_logs(self):
        """Retrieve YARN application logs. We use the `yarn` cli tool since
        it handles log aggregation well."""
        log.info('Attempting to aquire YARN application logs')
        log_request = ['sudo', 'yarn', 'logs', '-applicationId', self._appid]
        try:
            stdout, _ = self._run_ssh_on_master(log_request,
                                                'YARN log retrieval')
            file_path = self._write_to_log_file('yapp.log', stdout)
            log.info('  Wrote YARN application logs to %s', file_path)
        except StepFailedException:
            log.info('  Unable to retrieve YARN application logs :(')

    def _check_for_job_completion(self):
        log.info('Waiting for %s to complete...', self._appid)

        while True:
            time.sleep(self._opts['check_cluster_every'])

            app_info = self._get_application_info()

            # If the job is in not in a finished state then log the progress
            # and continue
            if app_info['state'] not in ('FINISHED', 'FAILED', 'KILLED'):
                # rounds the elapsed time to seconds for better formatting
                elapsed_ms = timedelta(milliseconds=app_info['elapsedTime'])
                elapsed_time = timedelta(seconds=elapsed_ms.seconds)
                progress = app_info['progress']
                log.info('  RUNNING for {0} with {1:.1f}% complete'
                         .format(elapsed_time, progress))
                continue

            # One of the following: UNDEFINED, SUCCEEDED, FAILED, KILLED
            final_status = app_info['finalStatus']
            if final_status == 'SUCCEEDED':
                log.info('  %s reached successful state!', self._appid)
                return
            else:
                log.info('  %s reached failure state %s :(', self._appid,
                         final_status)
                # Disable getting yarn logs
                # self._get_yarn_logs()
                tracking_url = app_info['trackingUrl']
                raise StepFailedException(
                        step_desc='Application {}'.format(self._appid),
                        reason='See logs at {}'.format(tracking_url))

    def _yrm_get(self, path, host=None, port=None, timeout=None):
        """Use curl to perform an HTTP GET on the given path on the
        YARN Resource Manager. Either return decoded JSON from the call,
        or raise an IOError

        *path* should not start with a '/'

        More info on the YARN REST API can be found here:

        https://hadoop.apache.org/docs/current/hadoop-yarn/
            hadoop-yarn-site/ResourceManagerRest.html
        """
        if host is None:
            host = self._address_of_master()

        if port is None:
            port = _YARN_RESOURCE_MANAGER_PORT

        if timeout is None:
            timeout = _YARN_API_TIMEOUT

        # using urljoin() to avoid a double / when joining host/port with path
        yrm_url = urljoin(
            'http://{}:{:d}'.format(host, port),
            '{}/{}'.format(_YRM_BASE_PATH, path)
        )

        curl_args = [
            'curl',  # always available on EMR
            '-fsS',  # fail on HTTP errors, print errors only to stderr
            '-m', str(timeout),  # timeout after 20 seconds
            yrm_url,
        ]

        stdout, stderr = self.fs.ssh._ssh_run(host, curl_args)

        return json.loads(stdout)
