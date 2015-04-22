import sys
sys.path.append('/Users/paolo/Documents/CRS4/src/pymine/PyMine/')

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp

class Mapper(api.Mapper):

    def map(self, context):
        raw_fields = context.value.split(',')
        context.emit(raw_fields[1], [raw_fields[0], raw_fields[2]])


class Reducer(api.Reducer):

    def reduce(self, context):
        line = []
        for value in context.values:
            line.append(value)
        context.emit(context.key, line)

def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer))
