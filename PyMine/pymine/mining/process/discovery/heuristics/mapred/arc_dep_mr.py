import sys
import uuid
import os
pymine_home = os.environ.get('PYMINE_HOME')
sys.path.append(pymine_home)
import collections
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pymine.mining.process.discovery.heuristics.dependency import DependencyMiner
from pymine.mining.process.discovery.heuristics import Matrix
SEPARATOR = '->'

class Mapper(api.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        context.setStatus("initializing mapper")
        self.process_id = uuid.uuid4()

    def get_key(self, item):
        return [item[0]]

    def map(self, context):
        case = context.value
        events_freq = Matrix()
        precede_matrix = Matrix()
        two_step_loop_freq = Matrix()
        start_events = Matrix()
        end_events = Matrix()
        long_distance_freq = Matrix()
        DependencyMiner.compute_precede_matrix_by_case(
            case, events_freq, precede_matrix, two_step_loop_freq, start_events, end_events, long_distance_freq)

        events = [e.name for e in case.events]

        for e1 in events:
            for e2 in events:
                context.emit(SEPARATOR.join([e1, e2]), [{
                    'precede': precede_matrix[e1][e2],
                    'two_step_loop': two_step_loop_freq[e1][e2],
                    'long_distance': long_distance_freq[e1][e2],
                    'start_node': e1,
                    'end_node': e2
                }])


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
        context.emit(arc_id, arc_info)


def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer))
