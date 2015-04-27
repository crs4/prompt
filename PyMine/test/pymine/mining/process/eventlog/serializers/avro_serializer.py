from pymine.mining.process.eventlog.factory import create_log_from_file
from pymine.mining.process.eventlog.serializers.avro_serializer import \
    serialize_log_as_case_collection, deserialize_log_from_case_collection
import unittest
import os


class AvroTest(unittest.TestCase):
    def test_ser_deser(self):
        # dest_file = "/tmp/log.avro"
        dest_file = os.path.join(os.path.dirname(__file__), '../../../../../../dataset/pg_4.avro')
        dataset_path = os.path.join(os.path.dirname(__file__), '../../../../../../dataset/pg_4.csv')
        log = create_log_from_file(dataset_path)[0]
        serialize_log_as_case_collection(log, dest_file)

        deser_log = deserialize_log_from_case_collection(dest_file)
        self.assertEqual(len(deser_log.cases), len(log.cases))
        for idx, c in enumerate(log.cases):
            deser_c = deser_log.cases[idx]
            self.assertEqual(deser_c.id, c.id)
            self.assertEqual(len(deser_c.events), len(c.events))
            for idx, e in enumerate(c.events):
                deser_e = deser_c.events[idx]
                self.assertEqual(e.name, deser_e.name)
                self.assertEqual(e.timestamp, deser_e.timestamp)
