"""
Implementation of the Heuristic Miner illustrated in http://wwwis.win.tue.nl/~wvdaalst/publications/p314.pdf
"""
# FIXME add reference to flexible cnet

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
# logger.setLevel(logging.DEBUG)


class Matrix(object):

    class Cell(object):
        def __init__(self, key, value):
            self.key = key
            self.value = value

        def __str__(self):
            return '%s: %s' % (self.key, self.value)

        def __repr__(self):
            return '%s: %s' % (self.key, self.value)

    class Column(object):
        def __init__(self):
            self._column = defaultdict(float)

        @property
        def cells(self):
            return [Matrix.Cell(k, v) for k, v in self._column.items()]

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
        self._2_step_loop = set()
        self._events = set()
        self._loop = defaultdict(int)
        self._self_loop = []
        self.cnet = None
        self._start_events = set()
        self._end_events = set()
        self.has_fake_start = self.has_fake_end = False
        self._events_freq = defaultdict(int)

    def _compute_precede_matrix(self):
        for case in self.log.cases:
            len_events = len(case.events)
            for i, event in enumerate(case.events):
                self._events_freq[event.activity_name] += 1
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
        for l, v in self._loop.items():
            if v >= dep_thr:
                node = self.cnet.get_node_by_label(l)
                self.cnet.add_arc(node, node)
                self._self_loop.append(node)

        for event in self._events:
            self._mine_dependency(event, 'input', dep_thr, relative_to_best)
            self._mine_dependency(event, 'output', dep_thr, relative_to_best)
            event_node = self.cnet.get_node_by_label(event)
            event_node.frequency = self._events_freq[event]

            # 2 step loops
            cells = self._2_step_loop_matrix[event].cells
            max_dep = max(cells, key=lambda x: x.value)
            candidate_dep = [c.key for c in cells if c.value >= dep_thr and max_dep.value - c.value <= relative_to_best]
            if not candidate_dep:
                if max_dep.value > 0:
                    candidate_dep.append(max_dep.key)

            for c in candidate_dep:
                c_node = self.cnet.get_node_by_label(c)
                self._2_step_loop.add(frozenset({c_node, event_node}))

                self.cnet.add_arc(c_node, event_node)
                self.cnet.add_arc(event_node, c_node)

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

        for l in self._self_loop:
            self.cnet.add_arc(l, l)

    def _mine_cnet(self, thr):
        output_bindings = Matrix()
        input_bindings = Matrix()

        for l in self._self_loop:
            self.cnet.add_input_binding(l, {l})
            self.cnet.add_output_binding(l, {l})

        for n1, n2 in self._2_step_loop:
            self.cnet.add_input_binding(n1, {n2})
            self.cnet.add_output_binding(n2, {n1})

        for c in self.log.cases:
            events = [e.activity_name for e in c.events]
            if self.cnet.has_fake_start:
                events.insert(0, self.cnet.fake_start_label)
            if self.cnet.has_fake_end:
                events.append(self.cnet.fake_end_label)

            logger.debug('events %s', events)

            for node in self.cnet.nodes:
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
                            logger.debug('i_idx %s value %s',i_idx, events[previous_id:idx][i_idx])
                        except ValueError:
                            continue
                        logger.debug('addings input %s to node %s', input_, node)
                        input_binding.add(input_)

                    if input_binding:
                        logger.debug('incrementing input_binding %s, node %s', input_binding, node)
                        input_bindings[node][frozenset(input_binding)] += 1

        # TODO: duplicated code...
        for n in self.cnet.nodes:
            total = sum(output_bindings[n].values())
            nodes_binded = set()
            for b, v in output_bindings[n].items():
                if v/total >= thr:
                    if len(b) > 1:
                        b = b - set(self.cnet.get_final_nodes())
                    self.cnet.add_output_binding(n, b)
                    nodes_binded |= b

            nodes_not_binded = n.output_nodes - nodes_binded
            for unlucky_node in nodes_not_binded:
                self.cnet.add_output_binding(n, {unlucky_node})

            total = sum(input_bindings[n].values())
            nodes_binded = set()

            for b, v in input_bindings[n].items():
                if v/total >= thr:
                    if len(b) > 1:
                        b = b - set(self.cnet.get_initial_nodes())
                    self.cnet.add_input_binding(n, b)
                    nodes_binded |= b
            nodes_not_binded = n.input_nodes - nodes_binded
            for unlucky_node in nodes_not_binded:
                self.cnet.add_input_binding(n, {unlucky_node})

    def mine(self, dependency_thr=0.5, and_thr=0.2, relative_to_best=0.1):
        if not self._precede_matrix:
            self._compute_precede_matrix()
        self._compute_dependency_matrix()
        self._mine_dependency_graph(dependency_thr, relative_to_best)
        self._mine_cnet(and_thr)
        return self.cnet


def main(file_path, dependency_thr, and_thr, relative_to_best):
    from pymine.mining.process.eventlog.factory import create_log_from_file
    from pymine.mining.process.eventlog.factory import SimpleProcessLogFactory
    from pymine.mining.process.tools.drawing.draw_cnet import draw
    from pymine.mining.process.conformance import simple_fitness
    log = create_log_from_file(file_path)[0]
    for c in log.cases:
        print c

    log = SimpleProcessLogFactory([
        ['a', 'b', 'c', 'd'],
        ['a', 'c', 'b', 'd'],
        ['a', 'c', 'e', 'd'],
        ['a', 'e', 'c', 'd'],
        ['a', 'b', 'e', 'd'],
        ['a', 'e', 'b', 'd'],
        # ['a', 'b', 'c', 'd', 'e'],
        # ['a', 'b', 'd', 'c', 'e'],
        # ['a', 'c', 'b', 'd', 'e'],
        # ['a', 'c', 'd', 'b', 'e'],
        # ['a', 'd', 'b', 'c', 'e'],
        # ['a', 'd', 'c', 'b', 'e'],
        # ['a', 'b1', 'b2', 'd'],
        # ['a', 'b2', 'b1', 'd'],
        # ['a', 'tb1', 'tb2', 'd'],
        # ['a', 'tb2', 'tb1', 'd'],
        # ['a', 'b1', 'tb2', 'd'],
        # ['a', 'tb2', 'b1', 'd'],
        # ['a', 'tb1', 'b2', 'd'],
        # ['a', 'b2', 'tb1', 'd'],
        #

    ]
    )
    hm = HeuristicMiner(log)
    cnet = hm.mine(dependency_thr, and_thr, relative_to_best)
    f = simple_fitness(log, cnet)
    print 'fitness', f.fitness
    print 'correct_cases', f.correct_cases
    print 'failed_cases', f.failed_cases
    print replay_case(log.cases[0], cnet)




    for n in cnet.nodes:
        print n.label, 'input_nodes', n.input_nodes
        print n.label, 'output_nodes', n.output_nodes

    draw_net_graph(hm.cnet)
    draw(hm.cnet)



if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('file_path', type=str, help='the path of the file containing the log')

    parser.add_argument('--rtb', type=float, default=0.1, help="relative to best")
    parser.add_argument('--bft', type=float, default=0.0, help="binding frequency threshold")
    parser.add_argument('--dt', type=float, default=0.5, help="dependency threshold")


    args = parser.parse_args()
    main(args.file_path, args.dt, args.bft, args.rtb)

