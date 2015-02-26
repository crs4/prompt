import unittest
from pymine.conformance import replay_case, simple_fitness
from pymine.mining.process.eventlog.factory import DictLogFactory
from test.pymine.mining.process.network.test_cnet import _create_cnet


class ConformanceTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ConformanceTestCase, self).__init__(*args, **kwargs)

        correct_log_dict = {
            'test': [['a', 'b', 'c',  'e']]
        }
        wrong_log_dict = {
            'test': [['a', 'b', 'c']]
        }
        half_correct_log_dict = {'test': correct_log_dict['test'] + wrong_log_dict['test']}

        self.factory = DictLogFactory(correct_log_dict)
        self.correct_log = self.factory.create_log()
        self.wrong_log = DictLogFactory(wrong_log_dict).create_log()
        self.half_correct_log = DictLogFactory(half_correct_log_dict).create_log()

    def test_replay_case_on_cnet(self):
        cnet, a, b, c, d, e = _create_cnet()
        log = self.factory.create_log()
        result, obligations, unknown = replay_case(log.cases[0], cnet)
        self.assertTrue(result)
        self.assertEqual(len(obligations), 0)

    def test_replay_wrong_case_on_cnet(self):
        cnet, a, b, c, d, e = _create_cnet()
        result, obligations, unknown = replay_case(self.wrong_log.cases[0], cnet)
        self.assertFalse(result)
        self.assertEqual(obligations, set([e]))

    def test_fitness_correct_log(self):
        cnet, a, b, c, d, e = _create_cnet()
        fitness_result = simple_fitness(self.correct_log, cnet)
        self.assertEqual(fitness_result.fitness, 1)

    def test_fitness_half_correct_log(self):
        cnet, a, b, c, d, e = _create_cnet()
        fitness_result = simple_fitness(self.half_correct_log, cnet)
        self.assertEqual(fitness_result.fitness, 0.5)












