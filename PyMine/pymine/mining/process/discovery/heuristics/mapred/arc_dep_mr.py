from collections import defaultdict
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pymine.mining.process.discovery.heuristics.dependency import DependencyMiner
from pymine.mining.process.discovery.heuristics import Matrix
from pymine.mining.process.discovery.heuristics.mapred import CLASSIFIER_FILENAME
from pymine.mining.mapred import deserialize_obj, CaseContext
import numpy as np
import logging
logger = logging.getLogger("mapred")
logger.addHandler(logging.FileHandler(filename='/tmp/mapred.log'))
logger.setLevel(logging.DEBUG)


class CustomCaseContext(CaseContext):

    def emit(self, key, value):
        if self.is_reducer():
            value = {
                'start_node': key[0],
                'end_node': key[1],
                'values': list(value)

            }
        super(CaseContext, self).emit(key, value)


class Mapper(api.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        context.setStatus("initializing mapper")
        self.classifier = deserialize_obj(context.job_conf.get(CLASSIFIER_FILENAME))

    def map(self, context):
        case = context.value
        events_freq = defaultdict(int)
        precede = Matrix()
        two_step_loop = Matrix()
        long_distance = Matrix()

        start_events = set()
        end_events = set()

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

        events = [e.name for e in case.events]
        set_events = set(events)

        for e1 in set_events:
            for e2 in set_events:
                logger.debug("emitting  e1 %s, e2 %s", e1, e2)
                """
                value is a list and not np.array for idempotency with combiner/reducer,
                infact final reducer cannot serialize in avro np.array but only list
                """
                value = np.array([
                    precede[e1][e2],
                    two_step_loop[e1][e2],
                    long_distance[e1][e2],
                    int(events.index(e1) == 0),  # is_start
                    int(events.index(e2) == len(events) - 1) # is_end
                ])
                logger.debug("emitting  e1 %s, e2 %s, value = %s", e1, e2, value)
                context.emit((e1, e2), value)


class Reducer(api.Reducer):

    def __init__(self, context):
        super(Reducer, self).__init__(context)
        context.set_status("initializing reducer")
        self.arcs = context.get_counter("DEP_MR", "ARCS")

    def reduce(self, context):
        logger.debug('context.key %s', context.key)
        start_node, end_node = context.key
        logger.debug('context.values %s', context.values)
        value = sum(context.values)
        # value = np.sum(list(context.values), axis=0)
        logger.debug('value %s', value)
        context.emit((start_node, end_node), value)


def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer, combiner_class=Reducer), private_encoding=True, context_class=CustomCaseContext)
