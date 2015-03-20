from pymine.mining.process.discovery.heuristic import HeuristicMiner
from pymine.mining.process.eventlog.factory import CsvLogFactory
from pmlab.cnet import force_graph
from collections import defaultdict
from pymine.mining.process.conformance.alignment import fitness
import logging
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s")
logger = logging.getLogger('alignment')
logger.setLevel(logging.DEBUG)


def main(csv_path, time_format, dep_thr, freq_thr):
    log_factory = CsvLogFactory(time_format=time_format)
    log = log_factory.create_log_from_file(csv_path)
    miner = HeuristicMiner()
    cnet = miner.mine(log, freq_thr, dep_thr)[0]
    inset = defaultdict(set)
    outset = defaultdict(set)
    nodes = []
    print 'nodes', cnet.nodes
    for node in cnet.nodes:
        print 'node %s: input_bindings %s' % (node, node.input_bindings)
        print 'node %s: output_bindings %s' % (node, node.output_bindings)

    print cnet.get_initial_nodes()
    print cnet.get_final_nodes()

    for n in cnet.nodes:
        nodes.append(n.label)
        for ip in n.input_bindings:
            inset[n.label].add(frozenset({i_n.label for i_n in ip.node_set}))

        for op in n.output_bindings:
            outset[n.label].add(frozenset({o_n.label for o_n in op.node_set}))

    s = force_graph.ForceDirectedGraph(nodes, inset, outset)
    s.run()

    print 'computing fitness...'
    f = fitness(log[log.processes[0]], cnet, max_depth=15)
    print 'fitness', f




if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create and show cnet mined from a csv log')
    parser.add_argument('csv_path', type=str, help='the path of the csv file containing the log')
    parser.add_argument('-t', type=str, default='%d-%m-%Y:%H.%M', help="time format")
    parser.add_argument('--ft', type=float, default=0.0, help="frequency threshold")
    parser.add_argument('--dt', type=float, default=0.0, help="dependency threshold")

    args = parser.parse_args()
    main(args.csv_path, args.t, args.ft, args.dt)