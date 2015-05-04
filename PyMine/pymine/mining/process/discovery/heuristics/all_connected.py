"""
Implementation of the Heuristic Miner illustrated in http://wwwis.win.tue.nl/~wvdaalst/publications/p314.pdf
and http://is.ieis.tue.nl/staff/aweijters/CIDM2010FHM.pdf
"""
from pymine.mining.process.discovery.heuristics.dependency import DependencyMiner
from pymine.mining.process.discovery.heuristics.binding_miner import BindingMiner


import logging
logger = logging.getLogger('all_connected')


class HeuristicMiner(object):
    def __init__(self, log, dep_miner_cls=None, b_miner_cls=None):
        self.log = log
        self.dep_miner = dep_miner_cls(log) if dep_miner_cls else DependencyMiner(log)
        self.binding_miner = b_miner_cls(log )if b_miner_cls else BindingMiner(log)

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

        cnet = self.dep_miner.mine(dependency_thr, relative_to_best, self_loop_thr, two_step_loop_thr, long_distance_thr)
        self.binding_miner.mine(cnet, bindings_thr)
        return cnet
