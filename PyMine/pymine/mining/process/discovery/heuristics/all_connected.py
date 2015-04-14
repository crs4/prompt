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

        def __iter__(self):
            return iter(self._column)

        def __getattr__(self, item):
            return getattr(self._column, item)

    def __init__(self):
        self._matrix = defaultdict(lambda: Matrix.Column())

    def __getitem__(self, item):
        return self._matrix[item]

    def __str__(self):
        return str(self._matrix)

    def __nonzero__(self):
        return bool(self._matrix)

    def __iter__(self):
        return iter(self._matrix)

    def __getattr__(self, item):
        return getattr(self._matrix, item)

    def get_column(self, item):
        cells = []
        for e in self._matrix:
            cells.append(Matrix.Cell(e, self._matrix[e][item]))
        return cells


class HeuristicMiner(object):


    def __init__(self, log):
        self.log = log
        self._precede_matrix = Matrix()
        self._dependency_matrix = Matrix()
        self._2_step_loop_matrix = Matrix()
        self._2_step_loop_freq = Matrix()
        self._events = set()
        self._loop = defaultdict(int)
        self.cnet = None
        self._start_events = set()
        self._end_events = set()
        self.has_fake_start = self.has_fake_end = False

    def _compute_precede_matrix(self):
        for case in self.log.cases:
            len_events = len(case.events)
            for i, event in enumerate(case.events):
                if i == 0:
                    self._start_events.add(event.activity_name)
                elif i == len_events - 1:
                    self._end_events.add(event.activity_name)

                self._events.add(event.activity_name)
                if i < len_events - 1:
                    self._precede_matrix[case.events[i].activity_name][case.events[i+1].activity_name] += 1
                if i < len_events - 2 and case.events[i].activity_name == case.events[i + 2].activity_name:
                    self._2_step_loop_freq[case.events[i].activity_name][case.events[i+1].activity_name] += 1

    def _compute_dependency_matrix(self):
        """
        Reversed matrix of the one illustrated in the paper
        :return:
        """
        for e in self._events:
            for next_e in self._events:
                l2_a_b = self._2_step_loop_freq[e][next_e]
                l2_b_a = self._2_step_loop_freq[next_e][e]
                self._2_step_loop_matrix[e][next_e] = (l2_b_a + l2_a_b)/(l2_a_b + l2_b_a + 1)

                p_a_b = self._precede_matrix[e][next_e]
                if e == next_e:
                    self._loop[e] = p_a_b/(p_a_b + 1)
                else:
                    p_b_a = self._precede_matrix[next_e][e]
                    self._dependency_matrix[e][next_e] = (p_b_a - p_a_b)/(p_a_b + p_b_a + 1)

    def _mine_dependency(self, event, dep_type, dep_thr, relative_to_best):
        if dep_type == 'input':
            cells = self._dependency_matrix[event].cells
        else:
            cells = self._dependency_matrix.get_column(event)

        logger.debug('cells of %s = %s', event, cells)
        max_dep = max(cells, key=lambda x: x.value)
        candidate_dep = [c.key for c in cells if c.value >= dep_thr and max_dep.value - c.value <= relative_to_best]
        logger.debug('candidate_dep %s of %s = %s', dep_type, event, candidate_dep)

        if not candidate_dep:
            if max_dep.value > 0:
                candidate_dep.append(max_dep.key)

        for c in candidate_dep:
            logger.debug('event %s, candidate_dep %s', event, candidate_dep)
            c_node = self.cnet.get_node_by_label(c)
            event_node = self.cnet.get_node_by_label(event)
            if dep_type == 'input':
                self.cnet.add_arc(c_node, event_node)
            else:
                self.cnet.add_arc(event_node, c_node)

    def _mine_dependency_graph(self, dep_thr, relative_to_best):
        self.cnet = CNet()
        self.cnet.add_nodes(*[e for e in self._events])
        logger.debug('self._events %s', self._events)
        self_loops = []
        for l, v in self._loop.items():
            if v >= dep_thr:
                node = self.cnet.get_node_by_label(l)
                self.cnet.add_arc(node, node)
                self_loops.append(node)

        for event in self._events:
            # l2_cells = self._2_step_loop_matrix[event].cells
            # for c in l2_cells:
            #     if c.value >= dep_thr:
            #         self.cnet.add_arc(self.cnet.get_node_by_label(event), self.cnet.get_node_by_label(c.key))
            self._mine_dependency(event, 'input', dep_thr, relative_to_best)
            self._mine_dependency(event, 'output', dep_thr, relative_to_best)


        start_nodes = self.cnet.get_initial_nodes()
        if not start_nodes or len(start_nodes) > 1:  # let's add a fake start
            fake_start = self.cnet.add_node(self.cnet.fake_start_label)
            self.cnet.has_fake_start = True

            for e in self._start_events:
                node = self.cnet.get_node_by_label(e)
                self._dependency_matrix[node][fake_start] = 1
                self.cnet.add_arc(fake_start, node)

        end_nodes = self.cnet.get_final_nodes()
        if not end_nodes or len(end_nodes) > 1:
            fake_end = self.cnet.add_node(self.cnet.fake_end_label)
            self.cnet.has_fake_end = True

            for e in self._end_events:
                node = self.cnet.get_node_by_label(e)
                self._dependency_matrix[fake_end][node] = 1
                self.cnet.add_arc(node, fake_end)

    def _comput_node_bindings(self, node, binding_type, and_thr):
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
        for node in self.cnet.nodes:
            self._comput_node_bindings(node, 'input', and_thr)
            self._comput_node_bindings(node, 'output', and_thr)

    def mine(self, dependency_thr=0.5, and_thr=0.5, relative_to_best=0.1):
        if not self._precede_matrix:
            self._compute_precede_matrix()
        self._compute_dependency_matrix()
        self._mine_dependency_graph(dependency_thr, relative_to_best)
        self._mine_cnet(and_thr)
        return self.cnet


def main(file_path, dependency_thr):
    from pymine.mining.process.eventlog.factory import create_log_from_file
    from pymine.mining.process.tools.drawing.draw_cnet import draw
    from pymine.mining.process.conformance import simple_fitness
    log = create_log_from_file(file_path)[0]
    hm = HeuristicMiner(log)
    cnet = hm.mine(dependency_thr)
    f = simple_fitness(log, cnet)
    print 'fitness', f.fitness
    print 'correct_cases', f.correct_cases
    print 'failed_cases', f.failed_cases
    print replay_case(log.cases[1], cnet)




    # for n in cnet.nodes:
        # print n.label, n.input_nodes, n.output_nodes
        # print n.label

    draw_net_graph(hm.cnet)
    draw(hm.cnet)



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

