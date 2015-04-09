from pymine.mining.process.discovery.heuristics.window import HeuristicMiner
from pymine.mining.process.eventlog.factory import create_log_from_file
from collections import defaultdict
from pymine.mining.process.conformance.alignment import fitness
from pymine.mining.process.conformance import simple_fitness, replay_case
from pymine.mining.process.tools.drawing.draw_cnet import draw

import logging
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s",
                    # level=logging.DEBUG
                    )
logger = logging.getLogger('heuristic')
# logger = logging.getLogger('cnet')
# logger.setLevel(logging.DEBUG)


def main(csv_path, freq_thr, dep_thr, window_size, binding_frequency_thr):
    log = create_log_from_file(csv_path)[0]
    for case in log.cases:
        print case


    miner = HeuristicMiner()
    cnet = miner.mine(log, freq_thr, dep_thr, binding_frequency_thr, window_size)
    inset = defaultdict(set)
    outset = defaultdict(set)
    nodes = []
    print 'nodes', cnet.nodes
    for node in cnet.nodes:
        print 'node %s: input_bindings %s ' % (node, node.input_bindings)
        print 'node %s: output_bindings %s ' % (node, node.output_bindings)

    print 'replay event __start'
    print cnet.replay_sequence(['__start', 'invite reviewers', 'invite reviewers'])

    #
    # print cnet.get_initial_nodes()
    # print cnet.get_final_nodes()

    for n in cnet.nodes:
        nodes.append(n.label)
        for ip in n.input_bindings:
            inset[n.label].add(frozenset({i_n.label for i_n in ip.node_set}))

        for op in n.output_bindings:
            outset[n.label].add(frozenset({o_n.label for o_n in op.node_set}))
    print 'inset', inset
    print 'outset', outset

    # print 'computing fitness...'
    # f = fitness(log, cnet, max_depth=15)
    # print 'fitness', f
    #
    print 'computing simple fitness...'
    f = simple_fitness(log, cnet)
    print 'fitness', f.fitness
    print 'f.correct_cases', f.correct_cases
    print 'f.failed_cases', f.failed_cases
    # logger.debug('***********************')
    # passed, obls, wat = cnet.replay_sequence(['a', 'c', 'd', 'e', 'f', 'd', 'c', 'e', 'f', 'c', 'd', 'e', 'h', 'z'])
    # print passed, obls, wat
    draw(cnet)
    from pymine.mining.process.network.converters.cnet_bpmn_converter import CNetBPMNConverter
    from pymine.mining.process.network.bpmn.serializer.bpmn2 import BPMN2Serializer

    converter = CNetBPMNConverter(cnet)
    bpmn = converter.convert_to_BPMN()
    for n in bpmn.nodes:
        print 'n.label', n.label
    serializer = BPMN2Serializer(bpmn)
    serializer.serialize()
    serializer.generate_bpmn_file('/tmp/test.bpmn')







if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create and show cnet mined from a csv log')
    parser.add_argument('csv_path', type=str, help='the path of the csv file containing the log')
    # parser.add_argument('-t', type=str, default='%d-%m-%Y:%H.%M', help="time format")
    parser.add_argument('--aft', type=float, default=0.0, help="arc frequency threshold")
    parser.add_argument('--bft', type=float, default=0.0, help="binding frequency threshold")
    parser.add_argument('--dt', type=float, default=0.5, help="dependency threshold")
    parser.add_argument('-w', type=int, default=3, help="window size")

    args = parser.parse_args()
    main(args.csv_path, args.aft, args.dt, args.w, args.bft)