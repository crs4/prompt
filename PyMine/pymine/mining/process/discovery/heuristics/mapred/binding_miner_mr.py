from pymine.mining.process.discovery.heuristics import Matrix
import pymine.mining.process.discovery.heuristics.binding_miner as bm


class BindingMiner(bm.BindingMiner):
    def mine(self, cnet, thr):
        output_bindings = Matrix()
        input_bindings = Matrix()

        for case in self.log.cases:
            self._mine_bindings_by_case(cnet, case, output_bindings, input_bindings)

        self._populate_cnet(cnet, input_bindings, output_bindings, thr)
