"""
Implementation of the Dependency Graph Miner illustrated in http://wwwis.win.tue.nl/~wvdaalst/publications/p314.pdf
"""
from collections import defaultdict
import logging

from pymine.mining.process.network.dependency import DependencyGraph

logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s",
                    level=logging.DEBUG
                    )
logger = logging.getLogger('heuristic')
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

        def __len__(self):
            return len(self._column)


    def __init__(self):
        self._matrix = defaultdict(lambda: Matrix.Column())

    def __getitem__(self, item):
        return self._matrix[item]

    def __str__(self):
        return str(self._matrix)

    def __len__(self):
        return len(self._matrix)


class DependencyMiner(object):

    def __init__(self, log=None):
        self.log = log
        self._precede_matrix = Matrix()
        self._dependency_matrix = Matrix()
        self._2_step_loop = Matrix()
        self._events = set()
        self._loop = defaultdict(int)
        self.d_graph = None

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

    @property
    def precede_matrix(self):
        if len(self._precede_matrix) == 0:
            self._compute_precede_matrix()
        return self._precede_matrix

    @property
    def dependency_matrix(self):
        if len(self._dependency_matrix) == 0:
            self._compute_dependency_matrix()
        return self._dependency_matrix

    def mine_dependency_graph(self, dep_thr=0.0, arc_freq=0):
        self._compute_precede_matrix()
        self._compute_dependency_matrix()
        self.d_graph = DependencyGraph()
        self.d_graph.add_nodes(*[e for e in self._events])
        logger.debug('self._events %s', self._events)
        for event in self._events:
            dep_cells = self._dependency_matrix[event].cells
            freq_cells = self._precede_matrix[event].cells
            logger.debug('cells of %s = %s', event, dep_cells)
            #candidate_dep = [c.key for c in dep_cells if c.value >= dep_thr]
            candidate_dep = []
            for i in xrange(0,len(dep_cells)):
                if dep_cells[i].value >= dep_thr and freq_cells[i].value >= arc_freq:
                    candidate_dep.append(dep_cells[i].key)
            logger.debug('candidate_dep of %s = %s', event, candidate_dep)

            if not candidate_dep and dep_cells:
                max_dep = max(dep_cells, key=lambda x: x.value)
                if max_dep.value > 0:
                    candidate_dep.append(max_dep.key)

            for c in candidate_dep:
                logger.debug('event %s, candidate_dep %s', event, candidate_dep)
                self.d_graph.add_arc(self.d_graph.get_node_by_label(c), self.d_graph.get_node_by_label(event))

        for l, v in self._loop.items():
            if v >= dep_thr:
                node = self.d_graph.get_node_by_label(l)
                self.d_graph.add_arc(node, node)
                
                
def main(file_path, dependency_thr):
    from pymine.mining.process.eventlog.factory import create_log_from_file
    log = create_log_from_file(file_path)[0]

    dm = DependencyMiner(log)
    logger.debug('MATRIX %s', str(dm.precede_matrix))

    dm.mine_dependency_graph(dependency_thr)

if __name__ == '__main__':
    import argparse
    logger.setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('file_path', type=str, help='the path of the file containing the log')

    parser.add_argument('--aft', type=float, default=0.0, help="arc frequency threshold")
    parser.add_argument('--bft', type=float, default=0.0, help="binding frequency threshold")
    parser.add_argument('--dt', type=float, default=0.0, help="dependency threshold")
    parser.add_argument('-w', type=int, default=None, help="window size")

    args = parser.parse_args()
    main(args.file_path, args.dt)