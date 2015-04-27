"""
Implementation of the Dependency Graph Miner illustrated in http://wwwis.win.tue.nl/~wvdaalst/publications/p314.pdf
"""
from collections import defaultdict
from pymine.mining.process.discovery.heuristics import Matrix
from pymine.mining.process.network.cnet import CNet
import logging
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s")
logger = logging.getLogger('depedency')


class DependencyMiner(object):

    def __init__(self, log):
        self.log = log
        self.precede_matrix = Matrix()
        self.dependency_matrix = Matrix()
        self.two_step_loop_matrix = Matrix()
        self.two_step_loop_freq = Matrix()
        self.two_step_loop = set()
        self.loop = defaultdict(int)
        self.self_loop = []
        self.start_events = set()
        self.end_events = set()
        self.has_fake_start = self.has_fake_end = False
        self.events_freq = defaultdict(int)
        self.long_distance_freq = Matrix()
        self.long_distance_matrix = Matrix()



    @property
    def events(self):
        return self.events_freq.keys()

    @staticmethod
    def compute_precede_matrix_by_case(
            case, events_freq, precede_matrix, two_step_loop_freq, start_events, end_events, long_distance_freq):

        events = [e.activity_name for e in case.events]
        len_events = len(events)
        for i, event in enumerate(events):
            events_freq[event] += 1
            if i == 0:
                start_events.add(event)
            elif i == len_events - 1:
                end_events.add(event)
            if i < len_events - 1:
                precede_matrix[events[i]][events[i+1]] += 1
            if i < len_events - 2 and events[i] == events[i + 2] and events[i] != events[i + 1]:
                two_step_loop_freq[events[i]][events[i+1]] += 1

            # long distance dependencies
            events_seen = set()
            for ld_event in events[i + 2:]:
                if event == ld_event:
                    break
                if ld_event in events_seen:
                    break
                events_seen.add(ld_event)
                long_distance_freq[event][ld_event] += 1

    def _compute_precede_matrix(self):

        for case in self.log.cases:
            self.compute_precede_matrix_by_case(
                case,
                self.events_freq,
                self.precede_matrix,
                self.two_step_loop_freq,
                self.start_events,
                self.end_events,
                self.long_distance_freq)

    def _compute_dependency_matrix(self):
        """
        Reversed matrix of the one illustrated in the paper
        :return:
        """
        for e in self.events:
            for next_e in self.events:
                if e != next_e:
                    # 2 step loops
                    l2_a_b = self.two_step_loop_freq[e][next_e]
                    l2_b_a = self.two_step_loop_freq[next_e][e]
                    self.two_step_loop_matrix[e][next_e] = (l2_b_a + l2_a_b)/(l2_a_b + l2_b_a + 1)

                    # long distance dependencies
                    card_a = self.events_freq[e]
                    card_b = self.events_freq[next_e]
                    a_ll_b = self.long_distance_freq[e][next_e]
                    self.long_distance_matrix[e][next_e] = 2*(a_ll_b - abs(card_a - card_b))/(card_a + card_b + 1)

                p_a_b = self.precede_matrix[e][next_e]
                if e == next_e:
                    self.loop[e] = p_a_b/(p_a_b + 1)
                else:
                    p_b_a = self.precede_matrix[next_e][e]
                    self.dependency_matrix[e][next_e] = (p_b_a - p_a_b)/(p_a_b + p_b_a + 1)

    def _mine_dependency(self, event, dep_type, dep_thr, relative_to_best, cnet):
        if dep_type == 'input':
            cells = self.dependency_matrix[event].cells
        else:
            cells = self.dependency_matrix.get_column(event)

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
                cnet.add_arc(c_node, event_node, frequency=self.precede_matrix[c.key][event], dependency=c.value)
            else:
                cnet.add_arc(event_node, c_node, frequency=self.precede_matrix[event][c.key], dependency=c.value)

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

                    if DependencyMiner._find_path_without_node(net, output_n, without_node, nodes_banned):
                        return True

            return False

    def mine(self, dep_thr, relative_to_best, self_loop_thr, two_step_loop_thr, long_distance_thr):
        if not self.precede_matrix:
            self._compute_precede_matrix()
        if not self.dependency_matrix:
            self._compute_dependency_matrix()

        cnet = CNet()
        cnet.add_nodes(*[e for e in self.events])
        logger.debug('self._events %s', self.events)

        # self loop
        for l, v in self.loop.items():
            if v >= self_loop_thr:
                node = cnet.get_node_by_label(l)
                cnet.add_arc(node, node, frequency=self.precede_matrix[l][l])
                self.self_loop.append(node)

        for event in self.events:
            self._mine_dependency(event, 'input', dep_thr, relative_to_best, cnet)
            self._mine_dependency(event, 'output', dep_thr, relative_to_best, cnet)
            event_node = cnet.get_node_by_label(event)
            event_node.frequency = self.events_freq[event]

            # 2 step loops
            cells = self.two_step_loop_matrix[event].cells
            candidate_dep = [c for c in cells if c.value >= two_step_loop_thr and c.key != event]

            for c in candidate_dep:
                c_node = cnet.get_node_by_label(c.key)
                self.two_step_loop.add(frozenset({c_node, event_node}))

                cnet.add_arc(c_node, event_node, frequency=self.precede_matrix[c.key][event], dependency=c.value)
                cnet.add_arc(event_node, c_node, frequency=self.precede_matrix[event][c.key], dependency=c.value)

        start_nodes = cnet.get_initial_nodes()
        if not start_nodes or len(start_nodes) > 1:  # let's add a fake start
            fake_start = cnet.add_node(cnet.fake_start_label)
            cnet.has_fake_start = True

            for e in self.start_events:
                node = cnet.get_node_by_label(e)
                self.dependency_matrix[node][fake_start] = 1
                cnet.add_arc(fake_start, node)

        end_nodes = cnet.get_final_nodes()
        if not end_nodes or len(end_nodes) > 1:
            fake_end = cnet.add_node(cnet.fake_end_label)
            cnet.has_fake_end = True

            for e in self.end_events:
                node = cnet.get_node_by_label(e)
                self.dependency_matrix[fake_end][node] = 1
                cnet.add_arc(node, fake_end)

        # long distance dependencies

        for event in self.events:
            node = cnet.get_node_by_label(event)
            cells = self.long_distance_matrix[event].cells
            candidate_dep = [c for c in cells if c.value >= long_distance_thr]

            for c in candidate_dep:
                c_node = cnet.get_node_by_label(c.key)
                if DependencyMiner._find_path_without_node(cnet, node, c_node):
                    cnet.add_arc(node, c_node, frequency=self.long_distance_freq[event][c], dependency=c.value)

        for l in self.self_loop:
            freq = cnet.get_arc_by_nodes(l, l).frequency
            cnet.add_input_binding(l, {l}, frequency=freq)
            cnet.add_output_binding(l, {l}, frequency=freq)

        logger.debug('self._2_step_loop %s', self.two_step_loop)
        for n1, n2 in self.two_step_loop:
            freq_n1_n2 = cnet.get_arc_by_nodes(n1, n2).frequency
            freq_n2_n1 = cnet.get_arc_by_nodes(n2, n1).frequency
            cnet.add_input_binding(n1, {n2}, frequency=freq_n2_n1)
            cnet.add_output_binding(n1, {n2}, frequency=freq_n1_n2)

            cnet.add_input_binding(n2, {n1}, frequency=freq_n1_n2)
            cnet.add_output_binding(n2, {n1}, frequency=freq_n2_n1)

        return cnet