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
"""Tests for YarnEMRJobRunner."""
import os

import boto3

from mrjob.yarnemr import YarnEMRJobRunner

from tests.mock_boto3 import MockBoto3TestCase
from tests.mr_null_spark import MRNullSpark
from tests.py2 import Mock
from tests.py2 import patch
from tests.sandbox import mrjob_conf_patcher


class YarnEMRJobRunnerTestBase(MockBoto3TestCase):

    def setUp(self):
        super(YarnEMRJobRunnerTestBase, self).setUp()

        self.client = boto3.client('emr')

        self.start(patch(
            'mrjob.bin.MRJobBinRunner._spark_submit_args',
            return_value=['<spark submit args>']))

    def _set_in_mrjob_conf(self, **kwargs):
        emr_opts = {'runners': {'yarnemr': {}}}
        emr_opts['runners']['yarnemr'].update(kwargs)
        patcher = mrjob_conf_patcher(emr_opts)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _default_mrjob_setup(self, **kwargs):
        cluster_cache = os.path.join(self.tmp_dir, 'cache')
        self._set_in_mrjob_conf(
            # yarn runner required
            ec2_key_pair_file='meh',
            expected_cores=1,
            expected_memory=10,
            yarn_logs_output_base=self.tmp_dir,
            # others
            cluster_cache_file=cluster_cache,
            pool_clusters=True,
            **kwargs
        )


class YarnEMRJobRunnerEndToEndTestCase(YarnEMRJobRunnerTestBase):

    def test_end_to_end(self):
        self._default_mrjob_setup()

        input1 = os.path.join(self.tmp_dir, 'input1')
        open(input1, 'w').close()
        input2 = os.path.join(self.tmp_dir, 'input2')
        open(input2, 'w').close()

        job = MRNullSpark(['-r', 'yarnemr', input1, input2])
        job.sandbox()

        with job.make_runner() as runner:
            # skip waiting
            runner._wait_for_cluster = Mock()

            # mock ssh
            runner.fs._ssh_run = Mock()
            mock_stderr = 'whooo stderr Submitting application application_15'\
                          '50537538614_0001 to ResourceManager stderr logs'
            runner.fs._ssh_run.return_value = ('meh stdour', mock_stderr)

            runner._get_application_info = Mock()
            runner._get_application_info.side_effect = [
                {
                  'state': 'NOT FINISHED',
                  'finalStatus': 'NOT SUCCESS',
                  'elapsedTime': 25.12345,
                  'progress': 5000
                },
                {
                  'state': 'FINISHED',
                  'finalStatus': 'SUCCEEDED',
                  'elapsedTime': 50.12345,
                  'progress': 10000
                },
            ]

            runner.run()


class YarnEMRJobRunnerClusterLaunchTestCase(YarnEMRJobRunnerTestBase):

    def _create_cluster(self):
        return self.client.run_job_flow(
            Instances=dict(
                InstanceCount=1,
                KeepJobFlowAliveWhenNoSteps=True,
                MasterInstanceType='m1.medium',
            ),
            JobFlowRole='fake-instance-profile',
            Name='Development Cluster',
            ReleaseLabel='emr-5.0.0',
            ServiceRole='fake-service-role',
        )['JobFlowId']

    def _setup_mocked_runner(self, setup_ret_val, state_ret_val):
        # create yarn runner
        runner = YarnEMRJobRunner()
        # don't try to create/wait for a cluster
        runner._create_cluster = Mock()
        runner._wait_for_cluster = Mock()
        # mock out stuff that is run after cluster management we
        # don't care about
        runner._address_of_master = Mock()
        runner.get_image_version = Mock()
        runner.get_image_version.return_value = 'meh'
        runner._execute_job = Mock()
        # mock out setup and state methods to always return true
        runner._compare_cluster_setup = Mock()
        runner._compare_cluster_setup.return_value = setup_ret_val
        runner._check_cluster_state = Mock()
        runner._check_cluster_state.return_value = \
            5 if state_ret_val else -1
        # return it
        return runner

    def test_valid_cluster_find(self):
        self._default_mrjob_setup()

        # create clusters and manually set them to WAITING
        cluster_ids = []
        for _ in range(2):
            cluster_id = self._create_cluster()
            self.mock_emr_clusters[cluster_id]['Status']['State'] = 'WAITING'
            cluster_ids.append(cluster_id)

        # mark all clusters as valid
        runner = self._setup_mocked_runner(True, True)

        # launch the job
        runner._launch_yarn_emr_job()

        # ensure we found and used this valid cluster
        self.assertIn(runner._cluster_id, cluster_ids)
        self.assertFalse(runner._created_cluster)

    def test_invalid_cluster_find(self):
        self._default_mrjob_setup()

        # create clusters and manually set them to WAITING
        cluster_ids = []
        for _ in range(2):
            cluster_id = self._create_cluster()
            self.mock_emr_clusters[cluster_id]['Status']['State'] = 'WAITING'
            cluster_ids.append(cluster_id)

        # mark all clusters as invalid
        runner = self._setup_mocked_runner(False, True)

        # launch the job
        runner._launch_yarn_emr_job()

        # ensure we created a new cluster
        self.assertTrue(runner._created_cluster)
