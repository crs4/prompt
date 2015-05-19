from pymine.mining.process.discovery.heuristics.all_connected import HeuristicMiner
from pymine.mining.process.eventlog.factory import create_log_from_file
from pymine.mining.process.tools.drawing.draw_cnet import draw
import pickle
import datetime as dt
from pymine.mining.process.eventlog.log import Classifier


MAPRED = 'mapred'
SEQ = 'seq'


def main(
        log_path,
        mode,
        dependency_thr,
        bindings_thr,
        rel_to_best,
        self_loop_thr,
        two_step_loop_thr,
        long_dist_thr,
        classifier,
        sep=Classifier.DEFAULT_SEP,
        fitness=None
):

    log = create_log_from_file(log_path) if isinstance(log_path, str) else log_path

    classifier_keys = classifier.split()
    classifier = Classifier(keys=classifier_keys, sep=sep) if classifier_keys else Classifier(sep=sep)
    if mode == MAPRED:
        from pymine.mining.process.discovery.heuristics.mapred.dependency_mr import DependencyMiner
        from pymine.mining.process.discovery.heuristics.mapred.bindings_mr import BindingMiner
        miner = HeuristicMiner(log, classifier, DependencyMiner, BindingMiner)
    else:
        miner = HeuristicMiner(log, classifier)
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
    base_path = "cnet_%s_%s__%s" % (mode, start_time, end_time)
    pkl_filepath = base_path + ".pkl"
    # pkl_filepath = "cnet_%s_%s__%s.pkl" % (mode, start_time, end_time, dependency_thr, bindings_thr, rel_to_best, self_loop_thr,two_step_loop_thr,long_dist_thr)
    with open(pkl_filepath, 'wb') as fitness_result:
        pickle.dump(cnet, fitness_result)



    print "mining started at", start_time
    print "mining finished at", end_time
    print 'time:', delta_t
    print 'cnet saved in', pkl_filepath
        
    if fitness:
        from pymine.mining.process.conformance import simple_fitness
        fitness_result = simple_fitness(log, cnet, classifier)
        print 'fitness', fitness_result.fitness
    else:
        fitness_result = None

    with open(base_path + '_report.txt', 'w') as fout:
        fout.write('\n'.join([
            'log_path %s' % log.filename,
            'mode %s' % mode,
            'dependency_thr %s' % dependency_thr,
            'bindings_thr %s' % bindings_thr,
            'rel_to_best %s' % rel_to_best,
            'self_loop_thr %s' % self_loop_thr,
            'two_step_loop_thr %s' % two_step_loop_thr,
            'long_dist_th %s' % long_dist_thr,
            'classifier %s' % classifier.keys,
            'start_time %s' % start_time,
            'end_time %s' % end_time,
            'delta_t %s' % delta_t,
            'fitness %s' % fitness_result.fitness if fitness_result else ''
        ]))
        # fout.write('----------------------\n')
        # fout.write('failed cases\n')
        # for case in fitness_result.failed_cases:
        #     fout.write('->'.join([classifier.get_event_name(e) for e in case.events]) + '\n')

    draw(cnet)
    return cnet, fitness_result


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create and show cnet mined from a csv log')
    parser = argparse.ArgumentParser(description='Create and show cnet mined from a csv log')
    parser.add_argument('log_path', type=str, help='the path of the log')
    parser.add_argument('mode', type=str, help='mining mode', choices=[MAPRED, SEQ])

    parser.add_argument('--bt', type=float, default=0.1, help="bindings threshold", dest='binding_th')
    parser.add_argument('--dt', type=float, default=0.5, help="dependency threshold", dest='dep_th')

    parser.add_argument('--slt', type=float, default=None, help="self loop threshold", dest='self_loop_th')
    parser.add_argument('--ldt', type=float, default=None, help="long distance threshold", dest='long_dist_th')
    parser.add_argument('--rtb', type=float, default=0.1, help="relative to the best threshold", dest='relative_best_th')
    parser.add_argument('--tsl', type=float, default=None, help="two steps loop threshold", dest='two_step_loop_th')
    parser.add_argument('-c', type=str, default="", help="classifier, string of attributes space separated", dest='classifier_keys')
    parser.add_argument('--cs', type=str, default=Classifier.DEFAULT_SEP, help="classifier separator", dest='classifier_separator')
    # parser.add_argument('-f', type=str, help="compute fitness", choices=['simple', 'aln'], dest='fitness')


    args = parser.parse_args()
    main(args.log_path,
         args.mode,
         args.dep_th,
         args.binding_th,
         args.relative_best_th,
         args.self_loop_th,
         args.two_step_loop_th,
         args.long_dist_th,
         args.classifier_keys,
         args.classifier_separator,
         )