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
import datetime
import os.path
import tempfile

import boto3

from mrjob.queue import SchedulingQueue
from mrjob.emr import EMRJobRunner
from tests.mock_boto3 import MockBoto3TestCase
from tests.py2 import patch


class SchedulingQueueTestCase(MockBoto3TestCase):

    def setUp(self):
        super(SchedulingQueueTestCase, self).setUp()

        # required_yarn_conf = {
        #     'cluster_cache_file': os.path.join(tempfile.mkdtemp(), 'cache'),
        #     'yarn_logs_output_base': '/dev/null',
        #     'ec2_key_pair_file': '/dev/null',
        #     'expected_memory': 1,
        #     'expected_cores': 1
        # }
        self.runner = EMRJobRunner()

        boto3.client('s3').create_bucket('fake')

    def _write_markers(self, curr_date, count, prefix, marker_type):
        for num in range(count):
            curr_date -= datetime.timedelta(seconds=5)
            date_str = curr_date.strftime('%Y-%m-%dT%H-%M%S%f')
            marker = 's3://fake/{}/{}_{}'.format(prefix, marker_type, date_str)
            self.runner.fs.danger_touchz(marker)
        return curr_date

    @patch('mrjob.emr.EMRJobRunner._create_cluster')
    @patch('mrjob.emr.EMRJobRunner._wait_for_cluster')
    def test_single_entry(self, mock_wait_for_cluster, mock_create_cluster):
        """One man is an island"""
        # enter the queue alone
        queue = SchedulingQueue(self.runner.fs, 's3://fake', 5, self.runner)
        queue.enter_cluster_creation_queue()

        # ensure we creation and wait for a cluster to come up
        self.assertEqual(mock_create_cluster.call_count, 1)
        self.assertEqual(mock_wait_for_cluster.call_count, 1)

        # check that all the markers are there
        s3 = boto3.resource('s3')
        bucket_contents = s3.mock_s3_fs['fake']
        markers = list(bucket_contents['keys'].keys())
        self.assertEqual(len(markers), 2)
        self.assertIn('attempt', [m.split('_')[0] for m in markers])
        self.assertIn('creation', [m.split('_')[0] for m in markers])

    @patch('mrjob.emr.EMRJobRunner._create_cluster')
    @patch('mrjob.emr.EMRJobRunner._wait_for_cluster')
    def test_late_arrival_success(self, mock_wait_for_cluster,
                                  mock_create_cluster):
        """Came late to the dinner party and succeeded"""
        queue_size = 5

        curr_date = datetime.datetime.now()
        self._write_markers(curr_date, queue_size, 'late1', 'attempt')

        # enter the queue and and write a success event after the
        # first sleep; ensure we properly exit the queue
        write_success = lambda n: self._write_markers(curr_date, 1,
                                                      'late1', 'creation')
        queue = SchedulingQueue(self.runner.fs, 's3://fake/late1',
                                queue_size, self.runner)
        with patch('time.sleep',
                   side_effect=write_success) as mock_sleep_time:
            result = queue.enter_cluster_creation_queue()
            self.assertTrue(result)
            # 480+240+120+60*6 = 1200 (9 counts before max)
            self.assertEqual(mock_sleep_time.call_count, 1)
            mock_create_cluster.assert_not_called()
            mock_wait_for_cluster.assert_not_called()

        # check the number of markers
        s3 = boto3.resource('s3')
        bucket_contents = s3.mock_s3_fs['fake']
        markers = list(bucket_contents['keys'].keys())
        self.assertEqual(len(markers), 7)

    @patch('mrjob.emr.EMRJobRunner._create_cluster')
    @patch('mrjob.emr.EMRJobRunner._wait_for_cluster')
    def test_late_arrival_timeout(self, mock_wait_for_cluster,
                                  mock_create_cluster):
        """Came late to the dinner party and timed-out"""
        queue_size = 5

        # write full attempts
        curr_date = datetime.datetime.now()
        self._write_markers(curr_date, queue_size, 'late2', 'attempt')

        # enter the queue and ensure we timed out since there is no creation
        queue = SchedulingQueue(self.runner.fs, 's3://fake/late2',
                                queue_size, self.runner)
        with patch('time.sleep') as mock_sleep_time:
            result = queue.enter_cluster_creation_queue()
            self.assertFalse(result)
            # 480+240+120+60*6 = 1200 (9 counts before max)
            self.assertEqual(mock_sleep_time.call_count, 9)
            mock_create_cluster.assert_not_called()
            mock_wait_for_cluster.assert_not_called()

        # check the number of markers
        # 6 = 5 attempts + new attempt
        s3 = boto3.resource('s3')
        bucket_contents = s3.mock_s3_fs['fake']
        markers = list(bucket_contents['keys'].keys())
        self.assertEqual(len(markers), 6)

    @patch('mrjob.emr.EMRJobRunner._create_cluster')
    @patch('mrjob.emr.EMRJobRunner._wait_for_cluster')
    @patch('time.sleep')
    def test_extra_creation(self, mock_sleep_time, mock_wait_for_cluster,
                            mock_create_cluster):
        """More the merrier"""
        # write attempts and creations
        curr_date = datetime.datetime.now()
        curr_date = self._write_markers(curr_date, 3, 'extra', 'creation')
        curr_date = self._write_markers(curr_date, 3, 'extra', 'attempt')

        # ensure we enter the queue and ignore the events
        queue = SchedulingQueue(self.runner.fs, 's3://fake/extra',
                                1, self.runner)
        queue.enter_cluster_creation_queue()

        self.assertEqual(mock_create_cluster.call_count, 1)
        self.assertEqual(mock_wait_for_cluster.call_count, 1)

        # check the number of markers
        # 8 = 3 creation + 3 attempts + new attempt + new creation
        s3 = boto3.resource('s3')
        bucket_contents = s3.mock_s3_fs['fake']
        markers = list(bucket_contents['keys'].keys())
        self.assertEqual(len(markers), 8)

    @patch('mrjob.emr.EMRJobRunner._create_cluster')
    @patch('mrjob.emr.EMRJobRunner._wait_for_cluster')
    def test_old_markers(self, mock_wait_for_cluster, mock_create_cluster):
        """Let the old die away"""
        # write attempts many hours ago
        curr_date = datetime.datetime.now() - datetime.timedelta(hours=2)
        curr_date = self._write_markers(curr_date, 20, 'extra', 'attempts')

        # ensure we enter the queue and ignore the old events
        queue = SchedulingQueue(self.runner.fs, 's3://fake/extra',
                                1, self.runner)
        queue.enter_cluster_creation_queue()

        self.assertEqual(mock_create_cluster.call_count, 1)
        self.assertEqual(mock_wait_for_cluster.call_count, 1)

        # check the number of markers
        # 22 = 20 old attempts + new attempt + new creation
        s3 = boto3.resource('s3')
        bucket_contents = s3.mock_s3_fs['fake']
        markers = list(bucket_contents['keys'].keys())
        self.assertEqual(len(markers), 22)
