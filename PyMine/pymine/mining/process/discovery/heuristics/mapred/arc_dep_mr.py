from collections import defaultdict
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pymine.mining.process.discovery.heuristics.dependency import DependencyMiner
from pymine.mining.process.discovery.heuristics import Matrix
from pymine.mining.process.discovery.heuristics.mapred import CLASSIFIER_FILENAME
from pymine.mining.mapred import deserialize_obj
from pymine.mining.process.eventlog.serializers.avro_serializer import convert_avro_dict_to_obj
from pydoop.avrolib import AvroContext
import logging
logger = logging.getLogger("mapred")
SEPARATOR = '->'


class Mapper(api.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        context.setStatus("initializing mapper")
        self.classifier = deserialize_obj(context.job_conf.get(CLASSIFIER_FILENAME))


    def map(self, context):
        case = convert_avro_dict_to_obj(context.value, 'Case')
        events_freq = defaultdict(int)
        mtrx = {
            'precede': Matrix(),
            'two_step_loop': Matrix(),
            'long_distance': Matrix()
        }

        start_events = set()
        end_events = set()
        DependencyMiner.compute_precede_matrix_by_case(
            case,
            self.classifier,
            events_freq, mtrx['precede'],
            mtrx['two_step_loop'],
            start_events,
            end_events,
            mtrx['long_distance']
        )

        events = [e.name for e in case.events]
        set_events = set(events)

        for e1 in set_events:
            for e2 in set_events:
                logger.debug("emitting  e1 %s, e2 %s")
                context.emit(SEPARATOR.join([e1, e2]), {
                    'precede': mtrx['precede'][e1][e2],
                    'two_step_loop': mtrx['two_step_loop'][e1][e2],
                    'long_distance': mtrx['long_distance'][e1][e2],
                    'is_start': int(events.index(e1) == 0),
                    'is_end': int(events.index(e2) == len(events) - 1)
                })


class Reducer(api.Reducer):

    def __init__(self, context):
        super(Reducer, self).__init__(context)
        context.set_status("initializing reducer")
        self.arcs = context.get_counter("DEP_MR", "ARCS")

    def reduce(self, context):
        arc_id = context.key
        start_node, end_node = arc_id.split(SEPARATOR)
        arc_info = {
            'precede': 0,
            'two_step_loop': 0,
            'long_distance': 0,
            'is_start': 0,
            'is_end': 0

        }

        for arc in context.values:
            for k in arc_info.keys():
                arc_info[k] += arc[k]
                arc_info["is_start"] += arc["is_start"]
                arc_info["is_end"] += arc["is_end"]

        arc_info['start_node'] = start_node
        arc_info['end_node'] = end_node

        print 'emitting arc_info', arc_info
        context.emit(SEPARATOR.join([start_node, end_node]), arc_info)


def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer), private_encoding=True, context_class=AvroContext)
