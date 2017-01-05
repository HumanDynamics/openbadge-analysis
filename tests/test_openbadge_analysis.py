#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_openbadge_analysis
----------------------------------

Tests for `openbadge_analysis` module.
"""


import sys
import unittest
import os
import json

# add the 'src' directory as one where we can import modules
src_dir = os.path.join(os.getcwd(), os.pardir)
sys.path.append(src_dir)

import openbadge_analysis as ob


class TestOpenbadge_analysis(unittest.TestCase):

    def setUp(self):
        self._meeting_audio_file_v1 = os.path.join(os.getcwd(), "data/meeting_audio_file_v1.txt")
        self._meeting_audio_file_v2 = os.path.join(os.getcwd(), "data/meeting_audio_file_v2.txt")
        pass

    def tearDown(self):
        pass

    def test_is_meeting_metadata(self):
        with open(self._meeting_audio_file_v1, 'r') as input_file:
            raw_data = input_file.readlines()
            meeting_metadata = json.loads(raw_data[0])
            self.assertTrue(ob.is_meeting_metadata(meeting_metadata))

        with open(self._meeting_audio_file_v2, 'r') as input_file:
            raw_data = input_file.readlines()
            meeting_metadata = json.loads(raw_data[0])
            self.assertTrue(ob.is_meeting_metadata(meeting_metadata))

        with open(self._meeting_audio_file_v1, 'r') as input_file:
            raw_data = input_file.readlines()
            not_metadata = json.loads(raw_data[1])
            self.assertFalse(ob.is_meeting_metadata(not_metadata))

    def test_meeting_log_version(self):
        with open(self._meeting_audio_file_v1, 'r') as input_file:
            raw_data = input_file.readlines()
            meeting_metadata = json.loads(raw_data[0])
            self.assertEqual(ob.meeting_log_version(meeting_metadata),'1.0')

        with open(self._meeting_audio_file_v2, 'r') as input_file:
            raw_data = input_file.readlines()
            meeting_metadata = json.loads(raw_data[0])
            self.assertEqual(ob.meeting_log_version(meeting_metadata), '2.0')


    def test_load_audio_chunks(self):
        chunks_from_v1 = ob.load_audio_chunks_as_json_objects(self._meeting_audio_file_v1)
        self.assertIsNotNone(chunks_from_v1)

        chunks_from_v2 = ob.load_audio_chunks_as_json_objects(self._meeting_audio_file_v2)
        self.assertIsNotNone(chunks_from_v2)

if __name__ == '__main__':
    sys.exit(unittest.main())
