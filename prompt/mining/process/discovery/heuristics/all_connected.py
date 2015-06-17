"""
Implementation of the Heuristic Miner illustrated in http://wwwis.win.tue.nl/~wvdaalst/publications/p314.pdf
and http://is.ieis.tue.nl/staff/aweijters/CIDM2010FHM.pdf
"""
from prompt.mining.process.discovery.heuristics.dependency import DependencyMiner
from prompt.mining.process.discovery.heuristics.binding_miner import BindingMiner
from prompt.mining.process.eventlog.log import Classifier

import logging
logger = logging.getLogger('all_connected')


class HeuristicMiner(object):
    def __init__(self, log, classifier=None, dep_miner=None, b_miner=None):
        self.classifier = classifier if classifier else Classifier()
        self.log = log
        self.dep_miner = dep_miner if dep_miner else DependencyMiner(log, classifier)
        self.binding_miner = b_miner if b_miner else BindingMiner(log, classifier)

        self.dep_miner.log = log
        self.binding_miner.log = log
        self.dep_miner.classifier = self.classifier
        self.binding_miner.classifier = self.classifier

    def mine(self, dependency_thr=0.5, bindings_thr=0.2, relative_to_best=0.1, self_loop_thr=None,
             two_step_loop_thr=None, long_distance_thr=None):
        """
        Mine a :class:`prompt.mining.process.network.cnet.CNet`. All possible thresholds range from 0 to 1.
        :param dependency_thr: dependency threshold
        :param bindings_thr: threshold for mining bindings
        :param relative_to_best: relative to the best threshold, for mining dependencies
        :param self_loop_thr: threshold for self loop, by default equals to dependency_thr
        :param two_step_loop_thr: threshold for two step loops, by default equals to dependency_thr
        :param long_distance_thr: threshold for long distance dependencies, by default equals to dependency_thr
        :return: :class:`prompt.mining.process.network.cnet.CNet`
        """
        self_loop_thr = self_loop_thr if self_loop_thr is not None else dependency_thr
        two_step_loop_thr = two_step_loop_thr if two_step_loop_thr is not None else dependency_thr
        long_distance_thr = long_distance_thr if long_distance_thr is not None else dependency_thr

        cnet = self.dep_miner.mine(dependency_thr, relative_to_best, self_loop_thr, two_step_loop_thr, long_distance_thr)
        self.binding_miner.mine(cnet, bindings_thr)
        return cnet
