from pymine.mining.process.discovery.heuristics import Matrix
import pymine.mining.process.discovery.heuristics.binding_miner as bm
from pymine.mining.mapred import MRLauncher, create_unique_filename, serialize_obj
import pickle
import pydoop.hdfs as hdfs
import os


class BindingMiner(bm.BindingMiner, MRLauncher):
    CNET_FILENAME = 'cnet_file'

    def mine(self, cnet, thr):
        output_bindings = Matrix()
        input_bindings = Matrix()

        cnet_filename = serialize_obj(cnet, 'cnet')

        cwd = os.path.dirname(__file__)
        output_dir_prefix = "bm_output"

        self.run_mapred_job(self.log,
                            os.path.join(cwd, 'binding.avsc'),
                            os.path.join(cwd, 'bindings_mr.py'),
                            "bindings_mr",
                            output_dir_prefix,
                            d_kwargs={BindingMiner.CNET_FILENAME: cnet_filename})

        for avro_obj in self.avro_outputs:
            if avro_obj["type"] == 'input':
                matrix = input_bindings
            else:
                matrix = output_bindings

            node = cnet.get_node_by_label(avro_obj["node"])
            binding = {cnet.get_node_by_label(b) for b in avro_obj["binding"]}
            matrix[node][frozenset(binding)] = avro_obj['freq']

        self._populate_cnet(cnet, input_bindings, output_bindings, thr)
