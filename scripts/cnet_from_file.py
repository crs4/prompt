from prompt.mining.process.eventlog.factory import create_log_from_file
from prompt.mining.process.conformance import simple_fitness
from prompt.mining.process.tools.drawing.draw_cnet import draw

import logging
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s",)
logger = logging.getLogger('heuristic')
# logger = logging.getLogger('cnet')
# logger.setLevel(logging.DEBUG)


def generic_computations(cnet, log):
    print 'computing simple fitness...'
    f = simple_fitness(log, cnet)
    print 'fitness', f.fitness
    print 'f.correct_cases', f.correct_cases
    print 'f.failed_cases', f.failed_cases
    print 'results', f.results

    from prompt.mining.process.network.converters.cnet_bpmn_converter import CNetBPMNConverter
    from prompt.mining.process.network.bpmn.serializer.bpmn2 import BPMN2Serializer

    converter = CNetBPMNConverter(cnet)
    bpmn = converter.convert_to_BPMN()
    serializer = BPMN2Serializer(bpmn)
    serializer.serialize()
    serializer.generate_bpmn_file('/tmp/test.bpmn')
    draw(cnet)


def all_connected(args):
    from prompt.mining.process.discovery.heuristics.all_connected import HeuristicMiner

    file_path = args.file_path
    dependency_thr = args.dt
    bindings_thr = args.bt
    relative_to_best = args.rtb
    self_loop_thr = args.slt
    two_step_loop_thr = args.tsl
    long_distance_thr = args.ldt

    log = create_log_from_file(file_path)[0]
    for case in log.cases:
        print case

    miner = HeuristicMiner(log)
    cnet = miner.mine(dependency_thr, bindings_thr, relative_to_best, self_loop_thr, two_step_loop_thr, long_distance_thr)
    generic_computations(cnet, log)


def window(args):
    from prompt.mining.process.discovery.heuristics.window import HeuristicMiner
    file_path = args.file_path
    arc_freq_thr = args.aft
    dep_thr = args.dt
    window_size = args.w
    binding_frequency_thr = args.bt

    miner = HeuristicMiner()
    log = create_log_from_file(file_path)[0]
    cnet = miner.mine(log, arc_freq_thr, dep_thr, binding_frequency_thr, window_size)
    generic_computations(cnet, log)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create and show cnet mined from a csv log')
    parser.add_argument('file_path', type=str, help='the path of the csv file containing the log')

    subparsers = parser.add_subparsers()
    parser_all_connected = subparsers.add_parser('all_connected')
    parser_window = subparsers.add_parser('window')

    parser_all_connected.add_argument('--bt', type=float, default=0.0, help="bindings threshold")
    parser_all_connected.add_argument('--dt', type=float, default=0.5, help="dependency threshold")

    parser_all_connected.add_argument('--slt', type=float, default=None, help="self loop threshold")
    parser_all_connected.add_argument('--ldt', type=float, default=None, help="long distance threshold")
    parser_all_connected.add_argument('--rtb', type=float, default=0.1, help="relative to the best threshold")
    parser_all_connected.add_argument('--tsl', type=float, default=None, help="two steps loop threshold")

    parser_window.add_argument('-w', type=int, default=3, help="window size")
    parser_window.add_argument('--aft', type=float, default=0.0, help="arc frequency threshold")
    parser_window.add_argument('--bt', type=float, default=0.0, help="bindings threshold")
    parser_window.add_argument('--dt', type=float, default=0.5, help="dependency threshold")

    parser_all_connected.set_defaults(func=all_connected)
    parser_window.set_defaults(func=window)

    args = parser.parse_args()
    args.func(args)