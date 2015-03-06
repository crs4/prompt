__author__ = 'paolo'

from pymine.mining.process.discovery import Miner as Miner
from pymine.mining.process.network import *
from pymine.mining.process.network.dependency import DependencyGraph as DependencyGraph
from pymine.mining.process.network.cnet import CNet as CNet
from pymine.mining.process.network.cnet import CNode as CNode

from pymine.mining.process.eventlog.factory import CsvLogFactory as CsvLogFactory
from pymine.mining.process.eventlog.factory import LogInfoFactory as LogInfoFactory

class HeuristicMiner(Miner):

    def __init__(self):
        pass

    def calculate_arcs_dependency(self, net):
        for arc in net.arcs:
            a = float(arc.frequency)
            if arc.start_node != arc.end_node:
                reversed_arc = arc.start_node.label+"->"+arc.end_node.label
                r_arc = net.get_arc_by_label(reversed_arc)
                if r_arc:
                    b = float(r_arc.frequency)
                    arc.dependency = abs((a-b)/(a+b+1.0))
                else:
                    arc.dependency = abs(a/(a+1.0))
            else:
                dep = abs(a/(a+1.0))
                arc.dependency = dep

    def prune_by_frequency(self, net, threshold):
        arcs_to_prune = []
        for arc in net.arcs:
            if arc.frequency < threshold:
                arcs_to_prune.append(arc)
        self._prune(net, arcs_to_prune)

    def prune_by_dependency(self, net, threshold):
        arcs_to_prune = []
        for arc in net.arcs:
            if arc.dependency < threshold:
                arcs_to_prune.append(arc)
        self._prune(net, arcs_to_prune)

    def _prune(self, net, arc):
        for a in arc:
            del net.arcs[a]

    def mine_dependency_graphs(self, log, frequency_threshold=None, dependency_threshold=None):
        graphs = []
        # for each process create a separate graph
        for process in log.processes:
            net = DependencyGraph()
            # for each activity create a graph node
            for activity in process.activities:
                net.add_node(label=activity.name)

            for case in process.cases:
                for e_index in xrange(0, len(case.events)-1):
                    start_event = case.events[e_index]
                    start_activity_instance = start_event.activity_instance
                    start_activity = start_activity_instance.activity
                    start_node_name = start_activity.name

                    end_event = case.events[e_index+1]
                    end_activity_instance = end_event.activity_instance
                    end_activity = end_activity_instance.activity
                    end_node_name = end_activity.name

                    arc_name = start_node_name+"->"+end_node_name
                    arc = net.get_arc_by_label(arc_name)
                    if arc:
                        arc.frequency += 1
                    else:
                        start_node = net.get_node_by_label(start_node_name)
                        end_node = net.get_node_by_label(end_node_name)
                        net.add_arc(start_node, end_node, arc_name, 1)

            self.calculate_arcs_dependency(net)

            if frequency_threshold:
                self.prune_by_frequency(net, frequency_threshold)
            if dependency_threshold:
                self.prune_by_dependency(net, dependency_threshold)
            graphs.append(net)
        return graphs

    def mine_cnets(self, log, frequency_threshold=0, dependency_threshold=0.0):
        log_info_factory = LogInfoFactory(log=log)
        log_info = log_info_factory.create_loginfo()
        dependency_graphs = self.mine_dependency_graphs(log, frequency_threshold, dependency_threshold)
        nets = []
        for process_info in log_info.processes_info:
            dep_net = dependency_graphs.pop()
            p_info = log_info.processes_info[process_info]
            c_net = CNet()
            for activity in dep_net.nodes:
                c_net.add_node(label=activity.label)
            for connection in dep_net.arcs:
                c_net.add_arc(c_net.get_node_by_label(connection.start_node.label),
                              c_net.get_node_by_label(connection.end_node.label),
                              label=connection.label,
                              frequency=connection.frequency)
            self.calculate_possible_binds(c_net, p_info.process, p_info.average_case_size)
            nets.append(c_net)
        return nets

    def calculate_possible_binds(self, c_net, process, window_size):
        # for each case in the process log check the activity position
        for node in c_net.nodes:
            input_binds = {}
            output_binds = {}
            for case in process.cases:
                candidate_input_binds = set()
                candidate_output_binds = set()
                found = False
                counter = 0
                for event in case.events:
                    activity = event.activity_instance.activity
                    try:
                        if not found:
                            if activity.name == node.label:
                                found = True
                                counter = 0
                            else:
                                arc_name = activity.name+"->"+node.label
                                arc = c_net.get_arc_by_label(arc_name)
                                if (arc in node.input_arcs) and (counter < window_size):
                                    candidate_input_binds.add(c_net.get_node_by_label(activity.name))
                                counter += 1
                        else:
                            arc_name = node.label+"->"+activity.name
                            arc = c_net.get_arc_by_label(arc_name)
                            if (arc in node.output_arcs) and (counter < window_size):
                                candidate_output_binds.add(c_net.get_node_by_label(activity.name))
                                counter += 1
                            #for candidate in candidate_input_binds:
                        for candidate in candidate_input_binds:
                            if candidate in input_binds:
                                input_binds[candidate] += 1
                            else:
                                input_binds[candidate] = 1
                        for candidate in candidate_output_binds:
                            if candidate in output_binds:
                                output_binds[candidate] += 1
                            else:
                                output_binds[candidate] = 1
                    except Exception, e:
                        print("Cannot compute bindins: "+str(e.message))
            for binds in input_binds:
                c_net.add_input_binding(node, {binds}, input_binds[binds])
            for binds in output_binds:
                c_net.add_output_binding(node, {binds}, output_binds[binds])

    def mine(self, log, frequency_threshold=0, dependency_threshold=0.0):
        dgraph = self.mine_dependency_graph(log, frequency_threshold=frequency_threshold, dependency_threshold=dependency_threshold)
        cnets = self.mine_cnets(dgraph, log)
        return cnets

    def mine_from_csv_file(self, filename, frequency_threshold=0, dependency_threshold=0.0):
        log_file = CsvLogFactory(input_filename=filename)
        log = log_file.create_log()
        return self.mine(log, frequency_threshold=frequency_threshold, dependency_threshold=dependency_threshold)