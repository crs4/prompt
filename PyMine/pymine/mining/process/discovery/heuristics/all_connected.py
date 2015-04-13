"""
Implementation of the Heuristic Miner illustrated in http://wwwis.win.tue.nl/~wvdaalst/publications/p314.pdf
"""

from pymine.mining.process.discovery import Miner as Miner
from numpy.matrixlib import matrix
from collections import defaultdict
from pymine.mining.process.network.dependency import DependencyGraph as DependencyGraph
from pymine.mining.process.network.cnet import CNet as CNet
from pymine.mining.process.conformance import replay_case

from pymine.mining.process.eventlog.factory import CsvLogFactory as CsvLogFactory
from pymine.mining.process.eventlog.factory import ProcessInfo
from pymine.mining.process.tools.hnet_miner import draw_net_graph
import logging
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s",
                    # level=logging.DEBUG
                    )
logger = logging.getLogger('all_connected')
# logger = logging.getLogger('cnet')
logger.setLevel(logging.DEBUG)


class Matrix(object):

    class Cell(object):
        def __init__(self, key, value):
            self.key = key
            self.value = value

        def __str__(self):
            return '%s: %s' %(self.key, self.value)

        def __repr__(self):
            return '%s: %s' %(self.key, self.value)

    class Column(object):
        def __init__(self):
            self._column = defaultdict(float)

        @property
        def cells(self):
            return [Matrix.Cell(k, v) for k,v in self._column.items()]

        def __getitem__(self, item):
            return self._column[item]

        def __setitem__(self, key, value):
            self._column[key] = value

        def __str__(self):
            return str(self._column)

        def __repr__(self):
            return repr(self._column)


    def __init__(self):
        self._matrix = defaultdict(lambda: Matrix.Column())

    def __getitem__(self, item):
        return self._matrix[item]

    def __str__(self):
        return str(self._matrix)

    def __nonzero__(self):
        return bool(self._matrix)

class HeuristicMiner(object):
    def __init__(self, log):
        self.log = log
        self._precede_matrix = Matrix()
        self._dependency_matrix = Matrix()
        self._2_step_loop = Matrix()
        self._events = set()
        self._loop = defaultdict(int)
        self.cnet = None

    def _compute_precede_matrix(self):
        for case in self.log.cases:
            len_events = len(case.events)
            for i, event in enumerate(case.events):
                self._events.add(event.activity_name)
                if i < len_events - 1:
                    self._precede_matrix[case.events[i].activity_name][case.events[i+1].activity_name] += 1
                if i < len_events - 2 and case.events[i].activity_name == case.events[i + 2].activity_name:
                    self._2_step_loop[case.events[i].activity_name][case.events[i+1].activity_name] += 1

    def _compute_dependency_matrix(self):
        """
        Reversed matrix of the one illustrated in the paper
        :return:
        """
        for event in self._events:
            for next_event in self._events:
                p_a_b = self._precede_matrix[event][next_event]
                if event == next_event:
                    self._loop[event] = p_a_b/(p_a_b + 1)
                else:
                    p_b_a = self._precede_matrix[next_event][event]
                    self._dependency_matrix[event][next_event] = (p_b_a - p_a_b )/(p_a_b + p_b_a + 1)

    def _mine_dependency_graph(self, dep_thr):
        self.cnet = CNet()
        self.cnet.add_nodes(*[e for e in self._events])
        logger.debug('self._events %s', self._events)
        for event in self._events:
            cells = self._dependency_matrix[event].cells
            logger.debug('cells of %s = %s', event, cells)
            candidate_dep = [c.key for c in cells if c.value >= dep_thr]
            logger.debug('candidate_dep of %s = %s', event, candidate_dep)

            if not candidate_dep:
                max_dep = max(cells, key=lambda x: x.value)
                if max_dep.value > 0:
                    candidate_dep.append(max_dep.key)

            for c in candidate_dep:
                logger.debug('event %s, candidate_dep %s', event, candidate_dep)
                self.cnet.add_arc(self.cnet.get_node_by_label(c), self.cnet.get_node_by_label(event))

        for l, v in self._loop.items():
            if v >= dep_thr:
                node = self.cnet.get_node_by_label(l)
                self.cnet.add_arc(node, node)

    def _compute_binding(self, binding_type, and_thr):
        for node in self.cnet.nodes:
            if binding_type == 'input':
                nodes = list(node.input_nodes)
            else:
                nodes = list(node.output_nodes)

            logger.debug('binding type %s, node %s, nodes %s', binding_type, node, nodes)
            if node in nodes:
                logger.debug('one loop found %s', node)
                nodes.remove(node)
                if binding_type == 'input':
                    self.cnet.add_input_binding(node, {node})
                else:
                    self.cnet.add_output_binding(node, {node})

            tmp_bindings = []
            if len(nodes) == 1:
                tmp_bindings.append({nodes[0]})
            else:
                for idx, o1 in enumerate(nodes):
                    for o2 in nodes[idx + 1:]:  # computing and_dependency
                        o1_o2 = self._precede_matrix[o1.label][o2.label]
                        o2_o1 = self._precede_matrix[o2.label][o1.label]
                        n_o1 = self._precede_matrix[node.label][o1.label]
                        n_o2 = self._precede_matrix[node.label][o2.label]
                        and_dep = (o1_o2 + o2_o1)/(n_o1 + n_o2 + 1)
                        logger.debug('------------')
                        logger.debug('node %s, o1 %s o2 %s', node, o1, o2)
                        logger.debug('o1_o2 %s, o2_o1 %s, n_o1 %s, n_o2 %s, and_dep %s', o1_o2, o2_o1, n_o1, n_o2, and_dep)

                        if and_dep >= and_thr:
                            binding = {o1, o2}
                            tmp_bindings.append(binding)
                        else:
                            tmp_bindings.append({o1})
                            tmp_bindings.append({o2})
            logger.debug('node %s, tmp_bindings %s', node, tmp_bindings)
            for idx, b in enumerate(tmp_bindings):
                candidate_bindings = []
                for other_b in tmp_bindings[idx + 1:]:
                    logger.debug('b %s, other_b %s', b, other_b)
                    if b & other_b:
                        candidate_bindings.append(other_b)

                logger.debug('node %s, candidate_bindings %s', node, candidate_bindings)
                if candidate_bindings:
                    intersection = set.intersection(*candidate_bindings)
                    logger.debug('b %s, intersection of %s: %s', b, candidate_bindings, intersection)
                    if intersection and b < intersection:
                        b |= intersection

            logger.debug('node %s, tmp_bindings %s', node, tmp_bindings)
            for b in tmp_bindings:
                if binding_type == 'input':
                    logger.debug('add_input_binding(%s, %s)', node, b)
                    self.cnet.add_input_binding(node, b)
                else:
                    logger.debug('add_output_binding(%s, %s)', node, b)
                    self.cnet.add_output_binding(node, b)

    def _mine_cnet(self, and_thr=0.5):
        self._compute_binding('input', and_thr)
        self._compute_binding('output', and_thr)

    def mine(self, dependency_thr=0.5, and_thr=0.5):
        if not self._precede_matrix:
            self._compute_precede_matrix()
        self._compute_dependency_matrix()
        self._mine_dependency_graph(dependency_thr)
        self._mine_cnet(and_thr)
        return self.cnet


def main(file_path, dependency_thr):
    from pymine.mining.process.eventlog.factory import create_log_from_file
    from pymine.mining.process.tools.drawing.draw_cnet import draw
    log = create_log_from_file(file_path)[0]
    hm = HeuristicMiner(log)
    cnet = hm.mine(dependency_thr)



    # for n in cnet.nodes:
        # print n.label, n.input_nodes, n.output_nodes
        # print n.label

    draw_net_graph(hm.cnet)
    draw(hm.cnet)
    for n in hm.cnet.nodes:
        logger.debug('n %s, input_b %s', n, n.input_bindings)
        logger.debug('n %s, output_b %s', n, n.output_bindings)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('file_path', type=str, help='the path of the file containing the log')

    parser.add_argument('--aft', type=float, default=0.0, help="arc frequency threshold")
    parser.add_argument('--bft', type=float, default=0.0, help="binding frequency threshold")
    parser.add_argument('--dt', type=float, default=0.5, help="dependency threshold")
    parser.add_argument('-w', type=int, default=None, help="window size")

    args = parser.parse_args()
    main(args.file_path, args.dt)

