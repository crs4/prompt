import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp
from pymine.mining.process.discovery.heuristics.mapred.binding_miner_mr import BindingMiner
from pymine.mining.mapred import deserialize_obj, CaseContext
from pymine.mining.process.discovery.heuristics import Matrix
from pymine.mining.process.discovery.heuristics.mapred import CLASSIFIER_FILENAME
import logging
logger = logging.getLogger("mapred")
SEPARATOR = '__'


class CustomCaseContext(CaseContext):
    def emit(self, key, value):
        if self.is_reducer():
            node, b_type, binding_nodes = key
            value = {'node': node, 'type': b_type, 'binding': list(binding_nodes), 'freq': value}

        super(CustomCaseContext, self).emit(key, value)


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

        for b_type in [input_bindings, output_bindings]:
            for node, bindings in b_type.items():
                for b in bindings:
                    serialized_b = tuple(sorted([n.label for n in b]))
                    str_b_type = 'input' if b_type == input_bindings else "output"
                    key = (node.label, str_b_type, serialized_b)
                    value = b_type[node][b]
                    context.emit(key, value)


class Reducer(api.Reducer):

    def __init__(self, context):
        super(Reducer, self).__init__(context)
        context.set_status("initializing reducer")

    def reduce(self, context):
        # node, b_type, binding_nodes = context.key
        total = sum(context.values)
        # context.emit(context.key, {'node': node, 'type': b_type, 'binding': binding_nodes, 'freq': total})
        context.emit(context.key, total)

def __main__():
    pp.run_task(pp.Factory(Mapper, Reducer, combiner_class=Reducer), private_encoding=True, context_class=CustomCaseContext)
