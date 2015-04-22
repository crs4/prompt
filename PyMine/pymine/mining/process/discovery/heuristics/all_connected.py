"""
Implementation of the Heuristic Miner illustrated in http://wwwis.win.tue.nl/~wvdaalst/publications/p314.pdf
and http://is.ieis.tue.nl/staff/aweijters/CIDM2010FHM.pdf
"""
from pymine.mining.process.discovery.heuristics import Matrix
from pymine.mining.process.conformance import replay_case
from pymine.mining.process.tools.hnet_miner import draw_net_graph
import logging
logger = logging.getLogger('all_connected')


class HeuristicMiner(object):
    def __init__(self, log, parallel_dep=False, parallel_binding=False):
        self.log = log
        if parallel_dep:
            raise Exception
        else:
            from pymine.mining.process.discovery.heuristics.dependency import DependencyMiner
            self.dep_miner = DependencyMiner(log)


    @staticmethod
    def _create_bindings(type_, node, bindings, cnet, thr):
        total = sum(bindings[node].values())
        nodes_binded = set()

        for b, v in bindings[node].items():
            if v/total >= thr:

                if type_ == 'output':
                    if len(b) > 1:
                        b = b - set(cnet.get_final_nodes())
                    cnet.add_output_binding(node, b, frequency=v)
                else:
                    if len(b) > 1:
                        b = b - set(cnet.get_initial_nodes())
                    cnet.add_input_binding(node, b, frequency=v)
                nodes_binded |= b

        nodes_not_binded = node.output_nodes - nodes_binded if type_ == 'output' else node.input_nodes - nodes_binded
        for unlucky_node in nodes_not_binded:
            if type_ == 'output':
                arc = cnet.get_arc_by_nodes(node, unlucky_node)
                cnet.add_output_binding(node, {unlucky_node}, frequency=arc.frequency)
            else:
                arc = cnet.get_arc_by_nodes(unlucky_node, node)
                cnet.add_input_binding(node, {unlucky_node}, frequency=arc.frequency)

    def _mine_bindings(self, cnet, thr):
        output_bindings = Matrix()
        input_bindings = Matrix()

        for l in self.dep_miner.self_loop:
            freq = cnet.get_arc_by_nodes(l, l).frequency
            cnet.add_input_binding(l, {l}, frequency=freq)
            cnet.add_output_binding(l, {l}, frequency=freq)

        logger.debug('self._2_step_loop %s', self.dep_miner.two_step_loop)
        for n1, n2 in self.dep_miner.two_step_loop:
            freq_n1_n2 = cnet.get_arc_by_nodes(n1, n2).frequency
            freq_n2_n1 = cnet.get_arc_by_nodes(n2, n1).frequency
            cnet.add_input_binding(n1, {n2}, frequency=freq_n2_n1)
            cnet.add_output_binding(n1, {n2}, frequency=freq_n1_n2)

            cnet.add_input_binding(n2, {n1}, frequency=freq_n1_n2)
            cnet.add_output_binding(n2, {n1}, frequency=freq_n2_n1)

        for c in self.log.cases:
            events = [e.activity_name for e in c.events]
            if cnet.has_fake_start:
                events.insert(0, cnet.fake_start_label)
            if cnet.has_fake_end:
                events.append(cnet.fake_end_label)

            for node in cnet.nodes:
                n_indexes = [idx_n_indexes for idx_n_indexes, j in enumerate(events) if j == node.label]
                inputs = node.input_nodes
                outputs = node.output_nodes
                for idx_n_indexes, idx in enumerate(n_indexes):
                    output_binding = set()
                    input_binding = set()

                    logger.debug('node %s, idx_n_indexes %s, idx %s, n_indexes %s', node, idx_n_indexes, idx, n_indexes)
                    logger.debug('node %s, outputs %s', node, outputs)
                    for output in outputs:
                        if output == node:
                            continue

                        next_idx = n_indexes[idx_n_indexes + 1] if idx_n_indexes < len(n_indexes) - 1 else len(events)
                        try:
                            o_idx = events[idx:next_idx].index(output.label)
                            logger.debug('o_idx %s, value %s', o_idx, events[idx:next_idx][o_idx])
                        except ValueError:
                            continue
                        output_binding.add(output)

                    if output_binding:
                        output_bindings[node][frozenset(output_binding)] += 1

                    logger.debug('node %s, inputs %s', node, inputs)
                    for input_ in inputs:
                        if input_ == node:
                            continue

                        logger.debug('node %s, input %s', node, input_)
                        logger.debug('idx_n_indexes %s, n_indexes %s', idx_n_indexes, n_indexes)
                        previous_id = 0 if idx_n_indexes == 0 else n_indexes[idx_n_indexes - 1]

                        try:
                            i_idx = events[previous_id:idx].index(input_.label)
                            logger.debug('i_idx %s value %s', i_idx, events[previous_id:idx][i_idx])
                        except ValueError:
                            continue
                        logger.debug('addings input %s to node %s', input_, node)
                        input_binding.add(input_)

                    if input_binding:
                        logger.debug('incrementing input_binding %s, node %s', input_binding, node)
                        input_bindings[node][frozenset(input_binding)] += 1

        for n in cnet.nodes:
            self._create_bindings('input', n, input_bindings, cnet, thr)
            self._create_bindings('output', n, output_bindings, cnet, thr)

    def mine(self, dependency_thr=0.5, bindings_thr=0.2, relative_to_best=0.1, self_loop_thr=None,
             two_step_loop_thr=None, long_distance_thr=None):
        """
        Mine a :class:`pymine.mining.process.network.cnet.CNet`. All possible thresholds range from 0 to 1.
        :param dependency_thr: dependency threshold
        :param bindings_thr: threshold for mining bindings
        :param relative_to_best: relative to the best threshold, for mining dependencies
        :param self_loop_thr: threshold for self loop, by default equals to dependency_thr
        :param two_step_loop_thr: threshold for two step loops, by default equals to dependency_thr
        :param long_distance_thr: threshold for long distance dependencies, by default equals to dependency_thr
        :return: :class:`pymine.mining.process.network.cnet.CNet`
        """
        self_loop_thr = self_loop_thr if self_loop_thr is not None else dependency_thr
        two_step_loop_thr = two_step_loop_thr if two_step_loop_thr is not None else dependency_thr
        long_distance_thr = long_distance_thr if long_distance_thr is not None else dependency_thr

        cnet = self.dep_miner.mine(dependency_thr, relative_to_best, self_loop_thr, two_step_loop_thr, long_distance_thr)
        self._mine_bindings(cnet, bindings_thr)
        return cnet
