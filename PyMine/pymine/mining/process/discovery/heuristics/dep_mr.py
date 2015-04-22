import sys
import uuid
sys.path.append('/Users/paolo/Documents/CRS4/src/pymine/PyMine/')

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp


class Mapper(api.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        context.setStatus("initializing mapper")
        self.process_id = uuid.uuid4()

    def get_key(self, item):
        return [item[0]]

    def map(self, context):
        values = []
        index = context.value.index('[')
        values_string = context.value[index:]
        unsorted_values = eval(values_string)
        sorted(unsorted_values, key=self.get_key)
        for value in unsorted_values:
            values.append(value[1])
        len_events = len(values)
        for i in xrange(len_events):
            if i < len_events - 1:
                key = values[i]+'->'+values[i+1]
                context.emit(key, ['_precede_matrix', 1])
            if i < len_events - 2 and values[i] == values[i + 2]:
                key = values[i]+'->'+values[i+1]
                context.emit(key, ['_2_step_loop', 1])


class Reducer(api.Reducer):

    def __init__(self, context):
        super(Reducer, self).__init__(context)
        context.set_status("initializing reducer")
        self.arcs = context.get_counter("DEP_MR", "ARCS")
        self.precede_value = 0
        self._2_step_loop = 0

    def reduce(self, context):
        arc_id = context.key
        arc_lines = context.values
        for arc_instance in arc_lines:
            type = arc_instance[0]
            value = arc_instance[1]
            if type == '_precede_matrix':
                self.precede_value += value
            elif type == '_2_step_loop':
                self._2_step_loop += value

        context.emit(arc_id, [self.precede_value, self._2_step_loop])
        context.increment_counter(self.arcs, 1)


def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer))
