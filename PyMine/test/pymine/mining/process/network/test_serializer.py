import unittest
from pymine.mining.process.network.serializer import *

class TestSerializer(unittest.TestCase):

    def __init__(self, label):
        super(TestSerializer, self).__init__(label)

    def test_serialize_as_string(self):
        serializer = Serializer()
        input_string = "input string"
        self.assertEqual(input_string, serializer.deserialize_from_string(serializer.serialize_as_string(input_string)))

    def deserialize_from_string(self):
        self.test_serialize_as_string()

    def serialize_as_file(self):
        self.assertTrue(True)

    def deserialize_from_file(self):
        self.assertTrue(True)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestSerializer('test_serialize_as_string'))
    suite.addTest(TestSerializer('deserialize_from_string'))
    suite.addTest(TestSerializer('serialize_as_file'))
    suite.addTest(TestSerializer('deserialize_from_file'))
    return suite

if __name__ == '__main__':
    runner = unittest.TestSerializer(verbosity=2)
    runner.run(suite())