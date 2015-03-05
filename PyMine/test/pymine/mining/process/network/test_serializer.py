import unittest
import json
from pymine.mining.process.network.serialization import *
from pymine.mining.process.network.cnet import *

class TestSerializer(unittest.TestCase):

    def __init__(self, label):
        super(TestSerializer, self).__init__(label)

    def create_cnet(self):
        cnet = CNet()
        node_a = cnet.add_node("A")
        node_b = cnet.add_node("B")
        node_c = cnet.add_node("C")
        node_d = cnet.add_node("D")
        cnet.add_arc(node_a, node_b, "A->B", 2)
        cnet.add_arc(node_b, node_c, "B->C", 1)
        cnet.add_arc(node_c, node_b, "C->B", 1)
        cnet.add_arc(node_b, node_d, "B->D", 2)
        cnet.add_output_binding(node_a, {node_b}, 2)
        cnet.add_output_binding(node_b, {node_c}, 1)
        cnet.add_output_binding(node_b, {node_d}, 2)
        cnet.add_output_binding(node_c, {node_b}, 1)
        cnet.add_input_binding(node_b, {node_a}, 2)
        cnet.add_input_binding(node_b, {node_c}, 1)
        cnet.add_input_binding(node_c, {node_b}, 1)
        cnet.add_input_binding(node_d, {node_b}, 2)
        return cnet

    def test_serialize_as_string(self):
        serializer = Serializer()
        input_string = json.dumps(self.create_cnet().get_json(), sort_keys=True, indent=2)
        self.assertEqual(input_string, serializer.deserialize_from_string(serializer.serialize_as_string(input_string)))

    def test_deserialize_from_string(self):
        self.test_serialize_as_string()

    def test_serialize_as_file(self):
        serializer = Serializer()
        filename = "test_serializer.json"
        input_string = json.dumps(self.create_cnet().get_json(), sort_keys=True, indent=2)
        serializer.serialize_as_file(input_string, filename)
        serialized_string = serializer.deserialize_from_file(filename)
        self.assertEqual(input_string, serialized_string)

    def test_deserialize_from_file(self):
        self.test_serialize_as_file()

    def test_serialize_as_json_file(self):
        serializer = Serializer()
        filename = "test_serializer.json"
        input_json = self.create_cnet().get_json()
        serializer.serialize_as_json_file(input_json, filename)
        deserialized_json = serializer.deserialize_json_from_file(filename)
        self.assertEqual(input_json, deserialized_json)

    def test_deserialize_json_from_file(self):
        self.test_serialize_as_json_file()


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestSerializer('test_serialize_as_string'))
    suite.addTest(TestSerializer('test_deserialize_from_string'))
    suite.addTest(TestSerializer('test_serialize_as_file'))
    suite.addTest(TestSerializer('test_deserialize_from_file'))
    return suite

if __name__ == '__main__':
    runner = unittest.TestSerializer(verbosity=2)
    runner.run(suite())