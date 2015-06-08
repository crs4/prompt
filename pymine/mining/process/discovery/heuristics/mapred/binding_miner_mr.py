from pymine.mining.process.discovery.heuristics import Matrix
import pymine.mining.process.discovery.heuristics.binding_miner as bm
from pymine.mining.mapred import MRLauncher, serialize_obj, CaseContext
from pymine.mining.process.discovery.heuristics.mapred import CLASSIFIER_FILENAME
import cPickle as pickle
import pydoop.hdfs as hdfs
import os


class BindingMiner(bm.BindingMiner, MRLauncher):
    def __init__(self, log, classifier=None, n_reducers=None, d_kwargs=None):
        bm.BindingMiner.__init__(self, log, classifier)
        n_reducers = 1 # FIXME
        MRLauncher.__init__(self, n_reducers, d_kwargs)

    CNET_FILENAME = 'cnet_file'

    def mine(self, cnet, thr):
        output_bindings = Matrix()
        input_bindings = Matrix()

        cnet_filename = serialize_obj(cnet, 'cnet')
        classifier_filename = serialize_obj(self.classifier, 'classifier')
        self.d_kwargs[BindingMiner.CNET_FILENAME] = cnet_filename
        self.d_kwargs[CLASSIFIER_FILENAME] = classifier_filename
        cwd = os.path.dirname(__file__)
        output_dir_prefix = "bm_output"

        self.run_mapred_job(self.log,
                            # os.path.join(cwd, 'binding.avsc'),
                            None,
                            os.path.join(cwd, 'bindings_mr.py'),
                            "bindings_mr",
                            output_dir_prefix,

                            )

        output_file = os.path.join(self.output_dir, 'part-r-00000')
        with hdfs.open(output_file, 'r') as f:
            pickled_string = f.read().split('\t')[1]
            mr_matrix = pickle.loads(pickled_string)

        for k, v in mr_matrix.iteritems():
            node, type_, binding = k

            node = cnet.get_node_by_label(node)
            binding = {cnet.get_node_by_label(b) for b in binding}
            if type_ == 'input':
                matrix = input_bindings
            else:
                matrix = output_bindings
            matrix[node][frozenset(binding)] = v

        self._populate_cnet(cnet, input_bindings, output_bindings, thr)
