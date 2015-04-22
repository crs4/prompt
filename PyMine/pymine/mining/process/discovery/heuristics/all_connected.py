"""
Implementation of the Heuristic Miner illustrated in http://wwwis.win.tue.nl/~wvdaalst/publications/p314.pdf
and http://is.ieis.tue.nl/staff/aweijters/CIDM2010FHM.pdf
"""
from collections import defaultdict
from pymine.mining.process.network.cnet import CNet as CNet
from pymine.mining.process.conformance import replay_case
from pymine.mining.process.tools.hnet_miner import draw_net_graph
import logging
logger = logging.getLogger('all_connected')


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
        self._loop = defaultdict(int)
        self._self_loop = []
        self._start_events = set()
        self._end_events = set()
        self.has_fake_start = self.has_fake_end = False
        self._events_freq = defaultdict(int)
        self._long_distance_freq = Matrix()
        self._long_distance_matrix = Matrix()

    @property
    def _events(self):
        return self._events_freq.keys()

    def _compute_precede_matrix(self):

        for case in self.log.cases:
            events = [e.activity_name for e in case.events]
            len_events = len(events)
            for i, event in enumerate(events):
                self._events_freq[event] += 1
                if i == 0:
                    self._start_events.add(event)
                elif i == len_events - 1:
                    self._end_events.add(event)
                if i < len_events - 1:
                    self._precede_matrix[events[i]][events[i+1]] += 1
                if i < len_events - 2 and events[i] == events[i + 2] and events[i] != events[i + 1]:
                    self._2_step_loop_freq[events[i]][events[i+1]] += 1

                # long distance dependencies
                events_seen = set()
                for ld_event in events[i + 2:]:
                    if event == ld_event:
                        break
                    if ld_event in events_seen:
                        break
                    events_seen.add(ld_event)
                    self._long_distance_freq[event][ld_event] += 1

    def _compute_dependency_matrix(self):
        """
        Reversed matrix of the one illustrated in the paper
        :return:
        """
        for e in self._events:
            for next_e in self._events:
                if e != next_e:
                    # 2 step loops
                    l2_a_b = self._2_step_loop_freq[e][next_e]
                    l2_b_a = self._2_step_loop_freq[next_e][e]
                    self._2_step_loop_matrix[e][next_e] = (l2_b_a + l2_a_b)/(l2_a_b + l2_b_a + 1)

                    # long distance dependencies
                    card_a = self._events_freq[e]
                    card_b = self._events_freq[next_e]
                    a_ll_b = self._long_distance_freq[e][next_e]
                    self._long_distance_matrix[e][next_e] = 2*(a_ll_b - abs(card_a - card_b))/(card_a + card_b + 1)

                p_a_b = self._precede_matrix[e][next_e]
                if e == next_e:
                    self._loop[e] = p_a_b/(p_a_b + 1)
                else:
                    p_b_a = self._precede_matrix[next_e][e]
                    self._dependency_matrix[e][next_e] = (p_b_a - p_a_b)/(p_a_b + p_b_a + 1)

    def _mine_dependency(self, event, dep_type, dep_thr, relative_to_best, cnet):
        if dep_type == 'input':
            cells = self._dependency_matrix[event].cells
        else:
            cells = self._dependency_matrix.get_column(event)

        logger.debug('cells of %s = %s', event, cells)
        max_dep = max(cells, key=lambda x: x.value)
        candidate_dep = [c for c in cells if c.value >= dep_thr and max_dep.value - c.value <= relative_to_best]
        logger.debug('candidate_dep %s of %s = %s', dep_type, event, candidate_dep)

        if not candidate_dep:
            if max_dep.value > 0:
                candidate_dep.append(max_dep)

        for c in candidate_dep:
            logger.debug('event %s, candidate_dep %s', event, candidate_dep)
            c_node = cnet.get_node_by_label(c.key)
            event_node = cnet.get_node_by_label(event)

            if dep_type == 'input':
                cnet.add_arc(c_node, event_node, frequency=self._precede_matrix[c.key][event], dependency=c.value)
            else:
                cnet.add_arc(event_node, c_node, frequency=self._precede_matrix[event][c.key], dependency=c.value)

    @staticmethod
    def _find_path_without_node(net, start_node, without_node, nodes_banned=None):
            logger.debug('start_node %s, without_node %s, nodes_banned %s', start_node, without_node, nodes_banned)
            nodes_banned = nodes_banned or {start_node, without_node}
            for output_n in start_node.output_nodes:
                    if output_n in nodes_banned:
                        continue
                    nodes_banned.add(output_n)
                    if output_n in net.get_final_nodes():
                        return True

                    if HeuristicMiner._find_path_without_node(net, output_n, without_node, nodes_banned):
                        return True

            return False

    def _mine_dependency_graph(self, dep_thr, relative_to_best, self_loop_thr, two_step_loop_thr, long_distance_thr):
        cnet = CNet()
        cnet.add_nodes(*[e for e in self._events])
        logger.debug('self._events %s', self._events)

        # self loop
        for l, v in self._loop.items():
            if v >= self_loop_thr:
                node = cnet.get_node_by_label(l)
                cnet.add_arc(node, node, frequency=self._precede_matrix[l][l])
                self._self_loop.append(node)

        for event in self._events:
            self._mine_dependency(event, 'input', dep_thr, relative_to_best, cnet)
            self._mine_dependency(event, 'output', dep_thr, relative_to_best, cnet)
            event_node = cnet.get_node_by_label(event)
            event_node.frequency = self._events_freq[event]

            # 2 step loops
            cells = self._2_step_loop_matrix[event].cells
            candidate_dep = [c for c in cells if c.value >= two_step_loop_thr and c.key != event]

            for c in candidate_dep:
                c_node = cnet.get_node_by_label(c.key)
                self._2_step_loop.add(frozenset({c_node, event_node}))

                cnet.add_arc(c_node, event_node, frequency=self._precede_matrix[c.key][event], dependency=c.value)
                cnet.add_arc(event_node, c_node, frequency=self._precede_matrix[event][c.key], dependency=c.value)

        start_nodes = cnet.get_initial_nodes()
        if not start_nodes or len(start_nodes) > 1:  # let's add a fake start
            fake_start = cnet.add_node(cnet.fake_start_label)
            cnet.has_fake_start = True

            for e in self._start_events:
                node = cnet.get_node_by_label(e)
                self._dependency_matrix[node][fake_start] = 1
                cnet.add_arc(fake_start, node)

        end_nodes = cnet.get_final_nodes()
        if not end_nodes or len(end_nodes) > 1:
            fake_end = cnet.add_node(cnet.fake_end_label)
            cnet.has_fake_end = True

            for e in self._end_events:
                node = cnet.get_node_by_label(e)
                self._dependency_matrix[fake_end][node] = 1
                cnet.add_arc(node, fake_end)

        # long distance dependencies

        for event in self._events:
            node = cnet.get_node_by_label(event)
            cells = self._long_distance_matrix[event].cells
            candidate_dep = [c for c in cells if c.value >= long_distance_thr]

            for c in candidate_dep:
                c_node = cnet.get_node_by_label(c.key)
                if HeuristicMiner._find_path_without_node(cnet, node, c_node):
                    cnet.add_arc(node, c_node, frequency=self._long_distance_freq[event][c], dependency=c.value)

        return cnet

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

        for l in self._self_loop:
            freq = cnet.get_arc_by_nodes(l, l).frequency
            cnet.add_input_binding(l, {l}, frequency=freq)
            cnet.add_output_binding(l, {l}, frequency=freq)

        logger.debug('self._2_step_loop %s', self._2_step_loop)
        for n1, n2 in self._2_step_loop:
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

        if not self._precede_matrix:
            self._compute_precede_matrix()
        if not self._dependency_matrix:
            self._compute_dependency_matrix()

        cnet = self._mine_dependency_graph(
            dependency_thr, relative_to_best, self_loop_thr, two_step_loop_thr, long_distance_thr)
        self._mine_bindings(cnet, bindings_thr)
        return cnet


def main(file_path, dependency_thr, and_thr, relative_to_best):
    from pymine.mining.process.eventlog.factory import create_log_from_file
    from pymine.mining.process.tools.drawing.draw_cnet import draw
    from pymine.mining.process.conformance import simple_fitness
    log = create_log_from_file(file_path)[0]
    for c in log.cases:
        print c

    # log = SimpleProcessLogFactory([
    #     ['a', 'b',  'd', 'e', 'g'],
    #     ['a', 'c',  'd', 'f', 'g']
    # ]
    # )
    hm = HeuristicMiner(log)
    # cnet = hm.mine(dependency_thr, and_thr, relative_to_best)
    cnet = hm.mine()
    f = simple_fitness(log, cnet)
    print 'fitness', f.fitness
    print 'correct_cases', f.correct_cases
    print 'failed_cases', f.failed_cases
    print replay_case(log.cases[0], cnet)

    for n in cnet.nodes:
        print n.label, 'input_nodes', n.input_nodes
        print n.label, 'output_nodes', n.output_nodes

    draw_net_graph(cnet)
    draw(cnet)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('file_path', type=str, help='the path of the file containing the log')

    parser.add_argument('--rtb', type=float, default=0.1, help="relative to best")
    parser.add_argument('--bft', type=float, default=0.0, help="binding frequency threshold")
    parser.add_argument('--dt', type=float, default=0.5, help="dependency threshold")
    args = parser.parse_args()
    main(args.file_path, args.dt, args.bft, args.rtb)