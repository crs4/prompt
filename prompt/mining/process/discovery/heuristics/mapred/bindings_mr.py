import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from prompt.mining.process.discovery.heuristics.mapred.binding_miner_mr import BindingMiner
from prompt.mining.mapred import deserialize_obj, CaseContext
from prompt.mining.process.discovery.heuristics import Matrix
from prompt.mining.process.discovery.heuristics.mapred import CLASSIFIER_FILENAME
from collections import defaultdict
import cPickle as pickle
import logging
logger = logging.getLogger("mapred")
SEPARATOR = '__'


class Mapper(api.Mapper):

    def __init__(self, context):
        super(Mapper, self).__init__(context)
        context.setStatus("initializing mapper")

        self.cnet = deserialize_obj(context.job_conf.get(BindingMiner.CNET_FILENAME))
        self.classifier = deserialize_obj(context.job_conf.get(CLASSIFIER_FILENAME))

    def map(self, context):
        case = context.value
        input_bindings = Matrix()
        output_bindings = Matrix()
        BindingMiner._mine_bindings_by_case(self.cnet, self.classifier, case, output_bindings, input_bindings)
        matrix = {}
        for b_type in [input_bindings, output_bindings]:
            for node, bindings in b_type.items():
                for b in bindings:
                    serialized_b = tuple(sorted([n.label for n in b]))
                    str_b_type = 'input' if b_type == input_bindings else "output"
                    key = (node.label, str_b_type, serialized_b)
                    matrix[key] = b_type[node][b]

        context.emit('', matrix)


class Combiner(api.Reducer):
    def __init__(self, context):
        super(Combiner, self).__init__(context)
        context.set_status("initializing reducer")

    def _emit(self, context, k, v):
        context.emit(k, v)

    def reduce(self, context):
        matrix = defaultdict(int)
        for m in context.values:
            for k, v in m.iteritems():
                matrix[k] += v
        self._emit(context, '', matrix)


class Reducer(Combiner):
    def _emit(self, context, k, v):
        context.emit(k, pickle.dumps(v))


def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer, combiner_class=Combiner), private_encoding=True, context_class=CaseContext,
                fast_combiner=True)
