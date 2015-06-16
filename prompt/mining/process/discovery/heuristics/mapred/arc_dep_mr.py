from collections import defaultdict
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.utils.misc import Timer
from prompt.mining.process.discovery.heuristics.dependency import DependencyMiner
from prompt.mining.process.discovery.heuristics import Matrix
from prompt.mining.process.discovery.heuristics.mapred import CLASSIFIER_FILENAME
from prompt.mining.mapred import deserialize_obj, CaseContext
import itertools as it
import numpy as np
import logging
import cPickle as pickle
logger = logging.getLogger("mapred")



def iterate_matrix(matrix):
    for k1, d1 in matrix.iteritems():
        for k2, v in d1.iteritems():
            yield k1, k2, v


class Mapper(api.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        context.setStatus("initializing mapper")
        self.classifier = deserialize_obj(context.job_conf.get(CLASSIFIER_FILENAME))
        self.timer = Timer(context, 'DEPMINER')

    def map(self, context):
        with self.timer.time_block('extract_case_from_context') as b:
            case = context.value
        
        with self.timer.time_block('init') as b:        
            events_freq = defaultdict(int)
            precede = Matrix()
            two_step_loop = Matrix()
            long_distance = Matrix()
            start_events = set()
            end_events = set()

        with self.timer.time_block('compute') as b:
            DependencyMiner.compute_precede_matrix_by_case(
                case,
                self.classifier,
                events_freq,
                precede,
                two_step_loop,
                start_events,
                end_events,
                long_distance
            )

        events = [self.classifier.get_event_name(e) for e in case.events]
        start_ev, end_ev = events[0], events[-1]
        counts = defaultdict(lambda: np.zeros(5))
        with self.timer.time_block('computing_dependencies'):
            for i, matrix in enumerate([precede, two_step_loop, long_distance]):
                for k1, k2, v in iterate_matrix(matrix):
                    key = (k1, k2)
                    # if not counts[key].all():
                    counts[key][3] = k1 == start_ev
                    counts[key][4] = k2 == end_ev
                    counts[key][i] = v

        with self.timer.time_block('global_emit'):
            context.emit("", counts)


class Combiner(api.Reducer):

    def __init__(self, context):
        super(Combiner, self).__init__(context)
        context.set_status("initializing reducer")
        self.arcs = context.get_counter("DEP_MR", "ARCS")
        self.timer = Timer(context, 'DEPMINER')

    def _emit(self, context, k, value):
        context.emit(k, value)

    def reduce(self, context):
        matrix = {}
        with self.timer.time_block('reduce_sum'):
            for v in context.values:
                for k in v:
                    if k in matrix:
                        matrix[k] += v[k]
                    else:
                        matrix[k] = v[k]
        # value = np.sum(list(context.values), axis=0)
            self._emit(context, "", matrix)


class Reducer(Combiner):

    def __init__(self, context):
        super(Reducer, self).__init__(context)
        context.set_status("initializing reducer")
        self.arcs = context.get_counter("DEP_MR", "ARCS")
        self.timer = Timer(context, 'DEPMINER')

    def _emit(self, context, k, value):
        value = pickle.dumps(value)
        super(Reducer, self)._emit(context, '', value)

def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer, combiner_class=Combiner), private_encoding=True, context_class=CaseContext, fast_combiner=True)
#    pp.run_task(pp.Factory(Mapper, Reducer), private_encoding=True, context_class=CustomCaseContext)
