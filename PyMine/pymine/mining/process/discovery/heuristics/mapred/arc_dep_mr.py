import sys
from collections import defaultdict
import os
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

sys.stderr.write("%r\n" % (os.listdir(os.getcwd(),)))
from pymine.mining.process.discovery.heuristics.dependency import DependencyMiner
from pymine.mining.process.discovery.heuristics import Matrix
from pydoop.avrolib import AvroContext
from pymine.mining.process.eventlog.serializers.avro_serializer import convert_avro_dict_to_obj
SEPARATOR = '->'
import logging
logger = logging.getLogger("mapred")


class Mapper(api.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        context.setStatus("initializing mapper")

    def map(self, context):
        case = convert_avro_dict_to_obj(context.value, 'Case')
        events_freq = defaultdict(int)
        precede_matrix = Matrix()
        two_step_loop_freq = Matrix()
        start_events = set()
        end_events = set()
        long_distance_freq = Matrix()
        DependencyMiner.compute_precede_matrix_by_case(
            case, events_freq, precede_matrix, two_step_loop_freq, start_events, end_events, long_distance_freq)

        events = [e.name for e in case.events]

        for e1 in events:
            for e2 in events:
                logger.debug("emitting  e1 %s, e2 %s")
                context.emit(SEPARATOR.join([e1, e2]), {
                    'precede': precede_matrix[e1][e2],
                    'two_step_loop': two_step_loop_freq[e1][e2],
                    'long_distance': long_distance_freq[e1][e2],
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

        }

        for arc in context.values:
            for k in arc_info.keys():
                arc_info[k] += arc[k]

        arc_info['start_node'] = start_node
        arc_info['end_node'] = end_node
        print 'emitting arc_info', arc_info
        context.emit(SEPARATOR.join([start_node, end_node]), arc_info)


def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer), private_encoding=True, context_class=AvroContext)
