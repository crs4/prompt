from pymine.mining.process.discovery.heuristics import Matrix
import logging
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s")
logger = logging.getLogger('binding')


class BindingMiner(object):
    def __init__(self, log):
        self.log = log

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

    def mine(self, cnet, thr):
        output_bindings = Matrix()
        input_bindings = Matrix()

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
