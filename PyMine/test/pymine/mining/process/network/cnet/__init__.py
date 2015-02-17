import unittest
from pymine.mining.process.network.cnet.cnet import CNet2
from pymine.mining.process.network.cnet.cnode import CNode
from pymine.mining.process.network.cnet.carc import CArc
import logging
# logging.basicConfig(level=logging.DEBUG)


class CNetTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(CNetTest, self).__init__(*args, **kwargs)
        self.cnet = CNet2()
        # self.a, self.b, self.c, self.d, self.e = [CNode(name=el) for el in ['a', 'b', 'c', 'd', 'e']]
        self.a, self.b, self.c, self.d, self.e = ['a', 'b', 'c', 'd', 'e']
        self.cnet.add_nodes(self.a, self.b, self.c, self.d, self.e)

        self.cnet.add_output_bindings(self.a, {self.b, self.c})
        self.cnet.add_output_bindings(self.a, {self.d})

        self.cnet.add_input_bindings(self.b, {self.a})
        self.cnet.add_output_bindings(self.b, {self.e, self.b})

        self.cnet.add_input_bindings(self.c, {self.a})
        self.cnet.add_output_bindings(self.c, {self.e})

        self.cnet.add_input_bindings(self.d, {self.a})
        self.cnet.add_output_bindings(self.d, {self.e})

        self.cnet.add_input_bindings(self.e, {self.b, self.c})
        self.cnet.add_input_bindings(self.e, {self.d})

    def test_get_initial_node(self):
        self.assertTrue(self.cnet.get_initial_node() == self.a)

    def test_get_final_node(self):
        self.assertTrue(self.cnet.get_final_node() == self.e)

    def test_replay(self):
        self.assertTrue(self.cnet.replay_case([self.a, self.d, self.e])[0])

    def test_replay_loop(self):
        self.assertTrue(self.cnet.replay_case([self.a, self.b, self.c, self.b, self.e])[0])
        self.assertTrue(self.cnet.replay_case([self.a, self.c, self.b,  self.e])[0])

    def test_replay_concurrency(self):
        self.assertTrue(self.cnet.replay_case([self.a, self.b, self.c,  self.e])[0])
        self.assertTrue(self.cnet.replay_case([self.a, self.c, self.b,  self.e])[0])

    def test_replay_failing(self):
        self.assertFalse(self.cnet.replay_case([self.a, self.d, self.d])[0])



if __name__ == '__main__':
    unittest.main()


