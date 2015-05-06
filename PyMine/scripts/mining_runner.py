from pymine.mining.process.discovery.heuristics.all_connected import HeuristicMiner
from pymine.mining.process.eventlog.factory import create_log_from_file
from pymine.mining.process.tools.drawing.draw_cnet import draw
import pickle
import datetime as dt


MAPRED = 'mapred'
SEQ = 'seq'


def main(
        log_path, mode, dependency_thr, bindings_thr, rel_to_best, self_loop_thr, two_step_loop_thr, long_dist_thr):
    log = create_log_from_file(log_path)
    if mode == MAPRED:
        from pymine.mining.process.discovery.heuristics.mapred.dependency_mr import DependencyMiner
        from pymine.mining.process.discovery.heuristics.mapred.bindings_mr import BindingMiner
        miner = HeuristicMiner(log, DependencyMiner, BindingMiner)
    else:
        miner = HeuristicMiner(log)
    start_time = dt.datetime.now()
    cnet = miner.mine(
        dependency_thr,
        bindings_thr,
        rel_to_best,
        self_loop_thr,
        two_step_loop_thr,
        long_dist_thr)
    end_time = dt.datetime.now()
    delta_t = end_time - start_time
    pkl_filepath = "cnet_%s_%s__%s.pkl" % (mode, start_time, end_time)
    with open(pkl_filepath, 'wb') as f:
        pickle.dump(cnet, f)
    print "mining started at", start_time
    print "mining finished at", end_time
    print 'time:', delta_t
    print 'cnet saved in', pkl_filepath

    draw(cnet)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create and show cnet mined from a csv log')
    parser.add_argument('log_path', type=str, help='the path of the log')
    parser.add_argument('mode', type=str, help='mining mode', choices=[MAPRED, SEQ])

    parser.add_argument('--bt', type=float, default=0.1, help="bindings threshold")
    parser.add_argument('--dt', type=float, default=0.5, help="dependency threshold")

    parser.add_argument('--slt', type=float, default=None, help="self loop threshold")
    parser.add_argument('--ldt', type=float, default=None, help="long distance threshold")
    parser.add_argument('--rtb', type=float, default=0.1, help="relative to the best threshold")
    parser.add_argument('--tsl', type=float, default=None, help="two steps loop threshold")

    args = parser.parse_args()
    main(args.log_path, args.mode, args.dt, args.bt, args.rtb, args.slt, args.tsl, args.ldt)