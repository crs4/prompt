import logging
logging.basicConfig(format="[%(levelname)s] %(asctime)s %(filename)s %(lineno)s: %(message)s", level=logging.INFO)

from pymine.mining.process.discovery.heuristics.all_connected import HeuristicMiner
from pymine.mining.process.eventlog.factory import create_log_from_file
from pymine.mining.process.eventlog.log import Classifier
from pymine.mining.process.discovery.heuristics.mapred.dependency_mr import DependencyMiner
from pymine.mining.process.discovery.heuristics.mapred.bindings_mr import BindingMiner
import pickle
import datetime as dt
import uuid
import os
import subprocess
import re
import time
from abc import ABCMeta, abstractmethod
import threading


MAPRED = 'mapred'
SEQ = 'seq'


def get_mapred_job(result_info):
    time.sleep(5)
    try:
        hadoop_prefix = os.environ.get('HADOOP_PREFIX', os.environ.get('HADOOP_HOME'))

        mapred_cmd = os.path.join(hadoop_prefix, 'bin/mapred')
        result_info['job'] = subprocess.check_output([mapred_cmd, 'job', '-list'])
    except Exception as ex:
        log.error('impossible to retrieve mapred job')
        raise ex

def _create_mapred_miner(log, classifier, n_reducers=None, d_kwargs=None):

    dp_miner = DependencyMiner(log, classifier, n_reducers, d_kwargs)
    b_miner = BindingMiner(log, classifier, n_reducers, d_kwargs)
    return HeuristicMiner(log, classifier, dp_miner, b_miner)

def _create_seq_miner(log, classifier):
    return HeuristicMiner(log, classifier)


class BaseRunner(object):
    __metaclass__ = ABCMeta

    def __init__(self, log, classifier, sep=Classifier.DEFAULT_SEP, run=1, draw=False, fitness_log=None):
        self.mode = ''
        self.log = log
        self.classifier = classifier
        self.run_results = {}
        self.n_run = run
        self.draw = draw
        self.fitness_log = fitness_log

    @abstractmethod
    def _create_miner(self):
        pass

    # @property
    # def base_path(self):
    #     return 'cnet_%s_%s_%s' % (self.mode, os.path.basename(log.filename), str(uuid.uuid4()))

    def run(self, dependency_thr, bindings_thr, rel_to_best, self_loop_thr, two_step_loop_thr, long_dist_thr):

        cwd = os.getcwd()
        node_info = ''
        if self.mode == 'mapred':
            hadoop_prefix = os.environ.get('HADOOP_PREFIX', os.environ.get('HADOOP_HOME'))
            if hadoop_prefix:
                yarn_cmd = os.path.join(hadoop_prefix, 'bin/yarn')
                node_info = subprocess.check_output([yarn_cmd, 'node','-list'])

        base_path = ''
        if node_info:
            nodes = re.findall('Total Nodes:(\d+)', node_info)
            if nodes:
                nodes = nodes[0]
                base_path = 'cnet_%s_%s_nodes_%s_%s' % (self.mode, os.path.basename(log.filename), nodes, str(uuid.uuid4()))

        if not base_path:
            base_path = 'cnet_%s_%s_%s' % (self.mode, os.path.basename(log.filename), str(uuid.uuid4()))

        for i in xrange(self.n_run):
            print 'run', i + 1
            miner = self._create_miner()
            start_time = dt.datetime.now()
            cnet = miner.mine(
                dependency_thr,
                bindings_thr,
                rel_to_best,
                self_loop_thr,
                two_step_loop_thr,
                long_dist_thr)

            self.run_results[i] = {}
            thread = threading.Thread(target=get_mapred_job, args=[self.run_results[i]])
            thread.run()

            end_time = dt.datetime.now()
            delta_t = end_time - start_time
            base_path_run = "%s_run_%s" % (base_path, i + 1)
            pkl_filepath = base_path_run + ".pkl"
            # pkl_filepath = "cnet_%s_%s__%s.pkl" % (mode, start_time, end_time, dependency_thr, bindings_thr, rel_to_best, self_loop_thr,two_step_loop_thr,long_dist_thr)
            with open(pkl_filepath, 'wb') as fitness_result:
                pickle.dump(cnet, fitness_result)

            print "mining started at", start_time
            print "mining finished at", end_time
            print 'time:', delta_t
            print 'cnet saved in', os.path.join(cwd, pkl_filepath)
            print node_info
            self.run_results[i].update({'start_time': start_time, 'end_time': end_time, 'delta_t': delta_t, 'cnet_file': pkl_filepath})

            fitness_result = None
            if self.fitness_log:
                from pymine.mining.process.conformance import simple_fitness
                fitness_result = simple_fitness(self.fitness_log, cnet, self.classifier)
                print 'fitness on %s: %s' % (self.fitness_log.filename, fitness_result.fitness)
                self.run_results[i]['fitness'] = fitness_result.fitness
            else:
                self.run_results[i]['fitness'] = None

        avg_time = sum([(r['end_time'] - r['start_time']).seconds for r in self.run_results.values()])/float(self.n_run)
        print 'avg time', avg_time

        report_filename = base_path + '_report.txt'
        with open(report_filename, 'w') as fout:
            fout.write('\n'.join([
                'log_path %s' % self.log.filename,
                'mode %s' % self.mode,
                'dependency_thr %s' % dependency_thr,
                'bindings_thr %s' % bindings_thr,
                'rel_to_best %s' % rel_to_best,
                'self_loop_thr %s' % self_loop_thr,
                'two_step_loop_thr %s' % two_step_loop_thr,
                'long_dist_th %s' % long_dist_thr,
                'classifier %s' % self.classifier.keys,
                'avg_time (sec)%s' % avg_time,
                # 'start_time %s' % start_time,
                # 'end_time %s' % end_time,
                # 'delta_t %s' % delta_t,
                # 'fitness %s' % fitness_result.fitness if fitness_result else ''
            ]))

            if node_info:
                fout.write('\n' + node_info + '\n')
            for i in xrange(self.n_run):
                fout.write('\n'.join([
                    '\n',
                    'run %s' % str(i + 1),
                    'cnet file %s' % os.path.join(cwd, self.run_results[i]['cnet_file']),
                    '\tstart_time %s' % self.run_results[i]['start_time'],
                    '\tend_time %s' % self.run_results[i]['end_time'],
                    '\tdelta_t %s' % self.run_results[i]['delta_t'],
                    '\tjob %s' % self.run_results[i].get('job', 'no info'),
                    # '\tfitness %s' % self.run_results[i]['fitness'],
                    ''
                ]))
                if  self.run_results[i]['fitness']:
                    fout.write('\n\t' + 'fitness on %s: %s' % (self.fitness_log.filename, self.run_results[i]['fitness']) + '\n')

        print 'report saved in', os.path.join(cwd, report_filename)
        if self.n_run and self.draw:
            from pymine.mining.process.tools.drawing.draw_cnet import draw
            draw(cnet)
        # return cnet, fitness_result


class SeqRunner(BaseRunner):
    def __init__(self, log, classifier, run=1, draw=False, fitness_log=None):
        super(SeqRunner, self).__init__(log, classifier, run=run, draw=draw, fitness_log=fitness_log)
        self.mode = 'seq'

    def _create_miner(self):
        return HeuristicMiner(self.log, self.classifier)



class MapRedRunner(BaseRunner):
    def __init__(self, log, classifier, run=1, draw=False, n_reducers=None, d_kwargs=None, fitness_log=None):
        super(MapRedRunner, self).__init__(log, classifier, run=run, draw=draw, fitness_log=fitness_log)
        self.mode = 'mapred'
        self.n_reducers = n_reducers
        self.d_kwargs = d_kwargs

    def _create_miner(self):
        dp_miner = DependencyMiner(self.log, self.classifier, self.n_reducers, self.d_kwargs)
        b_miner = BindingMiner(self.log, self.classifier, self.n_reducers, self.d_kwargs)
        return HeuristicMiner(self.log, self.classifier, dp_miner, b_miner)


def _add_basic_parser_argument(parser_):
    parser_.add_argument('log_path', type=str, help='the path of the log')
    # parser.add_argument('mode', type=str, help='mining mode', choices=[MAPRED, SEQ])

    parser_.add_argument('--bt', type=float, default=1, help="bindings threshold", dest='binding_th')
    parser_.add_argument('--dt', type=float, default=0.5, help="dependency threshold", dest='dep_th')

    parser_.add_argument('--slt', type=float, default=None, help="self loop threshold", dest='self_loop_th')
    parser_.add_argument('--ldt', type=float, default=None, help="long distance threshold", dest='long_dist_th')
    parser_.add_argument('--rtb', type=float, default=0.1, help="relative to the best threshold", dest='relative_best_th')
    parser_.add_argument('--tsl', type=float, default=None, help="two steps loop threshold", dest='two_step_loop_th')
    parser_.add_argument('-c', type=str, default="", help="classifier, string of attributes space separated", dest='classifier_keys')
    parser_.add_argument('--cs', type=str, default=Classifier.DEFAULT_SEP, help="classifier separator", dest='classifier_separator')
    parser_.add_argument('-r', type=int, default=1, help="how many run", dest='run')
    parser_.add_argument('-d', type=bool, default=False, help="draw last cnet", dest='draw')
    parser_.add_argument('-f', type=str, default=False, help="fitness log", dest='f_log')
    # parser.add_argument('-f', type=str, help="fitness log", dest='f_log', default="")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create and show cnet mined from a log file')
    subparsers = parser.add_subparsers(help='sub-command help')

    parser_mapred= subparsers.add_parser('mapred', help='run mining in mapred fashion')
    parser_mapred.set_defaults(mode='mapred')
    _add_basic_parser_argument(parser_mapred)

    parser_seq = subparsers.add_parser('seq', help='run mining in sequential fashion')
    parser_seq.set_defaults(mode='seq')
    _add_basic_parser_argument(parser_seq)

    parser_mapred.add_argument('-R', type=int, default=None, help="number of reducers", dest='reducers')
    parser_mapred.add_argument('-D', type=str, default=[], help="D kwargs", dest='d_kwargs', action='append')

    args = parser.parse_args()
    start_time = dt.datetime.now()
    log = create_log_from_file(args.log_path, create_process=False)
    end_time = dt.datetime.now()
    print 'time creating log %s' % (end_time - start_time).seconds
    classifier_keys = args.classifier_keys.split()
    sep = args.classifier_separator
    classifier = Classifier(keys=classifier_keys, sep=sep) if classifier_keys else Classifier(sep=sep)
    kwargs = dict(log=log, classifier=classifier, run=args.run, draw=args.draw)
    if args.f_log:
        fitness_log = create_log_from_file(args.f_log)
        kwargs['fitness_log'] = fitness_log

    if args.mode == 'seq':
        runner = SeqRunner(**kwargs)

    else:
        kwargs['n_reducers'] = args.reducers
        d_kwargs = {}
        for value in args.d_kwargs:
            k, v = value.split('=')
            d_kwargs[k] = v
        kwargs['d_kwargs'] = d_kwargs
        print kwargs

        runner = MapRedRunner(**kwargs)
    runner.run(
        args.dep_th,
        args.binding_th,
        args.relative_best_th,
        args.self_loop_th,
        args.two_step_loop_th,
        args.long_dist_th
    )