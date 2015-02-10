__author__ = 'paolo'

from pymine.mining.process.network.dependency.dgraph import DependencyGraph as DependencyGraph


class CNet(DependencyGraph):

    def __init__(self):
        super(CNet, self).__init__()
