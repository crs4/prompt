import unittest

from prompt.mining.process.conformance import replay_case, simple_fitness
from prompt.mining.process.eventlog.factory import create_process_log_from_list, create_log_from_file
from test.prompt.mining.process.network.test_cnet import _create_cnet
from prompt.mining.process.network.cnet import CNet
from prompt.mining.process.eventlog.log import Classifier
import os


class ConformanceTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ConformanceTestCase, self).__init__(*args, **kwargs)

        correct_cases = [['a', 'b', 'c',  'e']]
        wrong_cases = [['a', 'b', 'c']]

        half_correct_cases = correct_cases + wrong_cases

        self.correct_log = create_process_log_from_list(correct_cases)
        self.wrong_log = create_process_log_from_list(wrong_cases)
        self.half_correct_log = create_process_log_from_list(half_correct_cases)

    def test_replay_case_on_cnet(self):
        cnet, a, b, c, d, e = _create_cnet()
        result, obligations, unknown = replay_case(self.correct_log.cases[0], cnet)
        self.assertTrue(result)
        self.assertEqual(len(obligations), 0)
        self.assertEqual(a.frequency, 1)
        self.assertEqual(b.frequency, 1)
        self.assertEqual(c.frequency, 1)
        self.assertEqual(d.frequency, 0)
        self.assertEqual(e.frequency, 1)

        self.assertEqual(cnet.get_arc_by_nodes(a,b).frequency, 1)
        self.assertEqual(cnet.get_arc_by_nodes(a,c).frequency, 1)
        self.assertEqual(cnet.get_arc_by_nodes(a,d).frequency, 0)
        self.assertEqual(cnet.get_arc_by_nodes(b,e).frequency, 1)
        self.assertEqual(cnet.get_arc_by_nodes(c,e).frequency, 1)

        result, obligations, unknown = replay_case(self.correct_log.cases[0], cnet)

        self.assertEqual(a.frequency, 2)
        self.assertEqual(b.frequency, 2)
        self.assertEqual(c.frequency, 2)
        self.assertEqual(d.frequency, 0)
        self.assertEqual(e.frequency, 2)

        self.assertEqual(cnet.get_arc_by_nodes(a,b).frequency, 2)
        self.assertEqual(cnet.get_arc_by_nodes(a,c).frequency, 2)
        self.assertEqual(cnet.get_arc_by_nodes(a,d).frequency, 0)
        self.assertEqual(cnet.get_arc_by_nodes(b,e).frequency, 2)
        self.assertEqual(cnet.get_arc_by_nodes(c,e).frequency, 2)


    def test_replay_wrong_case_on_cnet(self):
        cnet, a, b, c, d, e = _create_cnet()
        result, obligations, unknown = replay_case(self.wrong_log.cases[0], cnet)
        self.assertFalse(result)
        self.assertEqual(len(obligations), 2)
        self.assertEqual(obligations[0].node, e)
        self.assertEqual(obligations[1].node, e)


    def test_fitness_correct_log(self):
        cnet, a, b, c, d, e = _create_cnet()
        fitness_result = simple_fitness(self.correct_log, cnet)
        self.assertEqual(fitness_result.fitness, 1)

    def test_fitness_half_correct_log(self):
        cnet, a, b, c, d, e = _create_cnet()
        fitness_result = simple_fitness(self.half_correct_log, cnet)
        self.assertEqual(fitness_result.fitness, 0.5)

    def test_fitness_with_classfier(self):
        cnet = CNet()
        sep = Classifier.DEFAULT_SEP
        a1_start = cnet.add_node('A1%sSTART' % sep)
        a1_end = cnet.add_node('A1%sEND' % sep)
        a2_end = cnet.add_node('A2%sEND' % sep)

        cnet.add_output_binding(a1_start, {a1_end})

        cnet.add_input_binding(a1_end, {a1_start})
        cnet.add_output_binding(a1_end, {a2_end})

        cnet.add_input_binding(a2_end, {a1_end})
        log_path = os.path.join(os.path.dirname(__file__), '../../../../../dataset/lifecycle.csv')

        log = create_log_from_file(log_path, False, False, False)
        cl = Classifier(keys=['activity', 'lifecycle'])
        fitness_result = simple_fitness(log, cnet, cl)
        self.assertEqual(fitness_result.fitness, 1)












