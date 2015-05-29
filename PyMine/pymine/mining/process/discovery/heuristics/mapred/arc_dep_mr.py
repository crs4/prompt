from collections import defaultdict
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pydoop.utils.misc import Timer
from pymine.mining.process.discovery.heuristics.dependency import DependencyMiner
from pymine.mining.process.discovery.heuristics import Matrix
from pymine.mining.process.discovery.heuristics.mapred import CLASSIFIER_FILENAME
from pymine.mining.mapred import deserialize_obj, CaseContext
import itertools as it
from collections import Counter
import numpy as np
import logging
logger = logging.getLogger("mapred")


class CustomCaseContext(CaseContext):

    def emit(self, key, value):
        if self.is_reducer():
            import pyavroc
            from pydoop.avrolib import AVRO_VALUE_OUTPUT_SCHEMA
            jc = self.get_job_conf()
            avtypes = pyavroc.create_types(jc.get(AVRO_VALUE_OUTPUT_SCHEMA))
            for k, vc in value.iteritems():
                v = {
                'start_node': k[0],
                'end_node': k[1],
                'values': list(vc)
                }
                av = avtypes.ArcInfo(**v)
                super(CustomCaseContext, self).emit(k, av)
        else:
            super(CustomCaseContext, self).emit(key, value)


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
        set_events = set(events)
	start_ev, end_ev = events[0], events[-1]
        counts =  {}
        with self.timer.time_block('global_emit') as b:
	    for (e1, e2) in it.product(set_events, set_events):
                value = np.array(
                       [
                        precede[e1][e2],
                        two_step_loop[e1][e2],
                        long_distance[e1][e2],
                        e1 == start_ev,  # is_start
                        e2 == end_ev # is_end
                       ], dtype=np.int32)
                counts[(e1, e2)] = value
            context.emit("", counts)


class Combiner(api.Reducer):

    def __init__(self, context):
        super(Combiner, self).__init__(context)
        context.set_status("initializing reducer")
        self.arcs = context.get_counter("DEP_MR", "ARCS")
        self.timer = Timer(context, 'DEPMINER')

    def reduce(self, context):
        matrix = {}
        with self.timer.time_block('reduce_sum') as b:
            for v in context.values:
                for k in v:
                    if k in matrix:
                        matrix[k] += v[k]
                    else:
                        matrix[k] = v[k]
        # value = np.sum(list(context.values), axis=0)
        context.emit("", matrix)


class Reducer(api.Reducer):

    def __init__(self, context):
        super(Reducer, self).__init__(context)
        context.set_status("initializing reducer")
        self.arcs = context.get_counter("DEP_MR", "ARCS")
        self.timer = Timer(context, 'DEPMINER')

    def reduce(self, context):
        matrix = {}
        with self.timer.time_block('reduce_sum') as b:
            for v in context.values:
                for k in v:
                    if k in zero:
                        matrix[k] += v[k]
                    else:
                        matrix[k] = v[k]
        # value = np.sum(list(context.values), axis=0)
        context.emit("", matrix)


def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer, combiner_class=Combiner), private_encoding=True, context_class=CustomCaseContext, fast_combiner=True)
#    pp.run_task(pp.Factory(Mapper, Reducer), private_encoding=True, context_class=CustomCaseContext)
