import networkx as nx
from pymine.mining.process.network.graph import BaseGraph


class Graph(BaseGraph):
    def __init__(self):
        self._graph = nx.DiGraph()

    def add_node(self, node):
        return self._graph.add_node(node)

    def add_edge(self, start_node, end_node, data=None):
        return self._graph.add_edge(start_node, end_node, data)

    def shortest_path(self, start, end, attribute=None):
        return nx.bidirectional_dijkstra(self._graph, start, end, attribute)

    def nodes(self):
        return self._graph.nodes()

    def edges(self):
        return self._graph.edges()



