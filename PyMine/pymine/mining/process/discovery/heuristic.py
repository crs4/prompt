__author__ = 'paolo'

from pymine.mining.process.discovery import Miner as Miner
from pymine.mining.process.network.dependency.dgraph import DependencyGraph as DependencyGraph
from pymine.mining.process.network.dependency.dnode import DNode as DNode
from pymine.mining.process.network.dependency.darc import DArc as DArc
from pymine.mining.process.network.cnet.cnet import CNet as CNet
from pymine.mining.process.network.cnet.cnode import CNode as CNode
from pymine.mining.process.network.cnet.carc import CArc as CArc

from pymine.mining.process.eventlog.factory import CsvLogFactory as CsvLogFactory
from pymine.mining.process.eventlog.factory import LogInfoFactory as LogInfoFactory

class HeuristicMiner(Miner):

    def __init__(self):
        pass

    def calculate_arcs_dependency(self, net):
        for arc in net.arcs:
            if net.arcs[arc].input_node != net.arcs[arc].output_node:
                reversed_arc = net.nodes[net.arcs[arc].output_node].name+"->"+net.nodes[net.arcs[arc].input_node].name
                a = float(net.arcs[arc].frequency)
                if reversed_arc in net.arcs:
                    b = float(net.arcs[reversed_arc].frequency)
                    net.arcs[arc].dependency = abs((a-b)/(a+b+1.0))
                else:
                    net.arcs[arc].dependency = abs(a/(a+1.0))
            else:
                a = float(net.arcs[arc].frequency)
                net.arcs[arc].dependency = abs(a/(a+1.0))

    def prune_by_frequency(self, net, threshold):
        keys = []
        for arc in net.arcs:
            if net.arcs[arc].frequency < threshold:
                keys.append(arc)
        self._prune(net, keys)

    def prune_by_dependency(self, net, threshold):
        keys = []
        for arc in net.arcs:
            if net.arcs[arc].dependency < threshold:
                keys.append(arc)
        self._prune(net, keys)

    def _prune(self, net, keys):
        for k in keys:
            del net.arcs[k]

    def mine_dependency_graph(self, log, frequency_threshold=None, dependency_threshold=None):
        net = DependencyGraph()

        for activity in log.activities:
            activity_node = DNode()
            activity_node.name = log.activities[activity].name
            net.nodes[activity_node.name] = activity_node

        for case in log.cases:
            for e_index in xrange(0, len(log.cases[case].events)-1):
                start_event_index = log.cases[case].events[e_index]
                start_activity_instance = log.activity_instances[log.events[start_event_index].activity_instance_id]
                start_activity = log.activities[start_activity_instance.activity_id]
                start_node = start_activity.name

                end_event_index = log.cases[case].events[e_index+1]
                end_activity_instance = log.activity_instances[log.events[end_event_index].activity_instance_id]
                end_activity = log.activities[end_activity_instance.activity_id]
                end_node = end_activity.name

                arc_name = start_node+"->"+end_node
                if arc_name in net.arcs:
                    net.arcs[arc_name].frequency += 1
                else:
                    arc = DArc(name=arc_name, input_node=start_node, output_node=end_node)
                    arc.frequency = 1
                    net.arcs[arc_name] = arc
                net.nodes[start_node].output_arcs.append(arc_name)
                net.nodes[end_node].input_arcs.append(arc_name)

        self.calculate_arcs_dependency(net)

        if frequency_threshold:
            self.prune_by_frequency(net, frequency_threshold)
        if dependency_threshold:
            self.prune_by_dependency(net, dependency_threshold)

        return net

    def mine_cnets(self, depnet, log):
        log_info_factory = LogInfoFactory(log=log)
        log_info = log_info_factory.create_loginfo()
        nets = {}
        for process in log_info.processes_info:
            pinfo = log_info.processes_info[process]
            cnet = CNet()
            for activity in depnet.nodes:
                input_binds, output_binds = self.calculate_possible_binds(process, depnet.nodes[activity], log, pinfo.average_case_size)
                node = CNode(id=activity, name=activity)
                node.input_bindings = input_binds
                node.output_bindings = output_binds
                cnet.nodes[activity] = node
            for connection in depnet.arcs:
                arc = CArc(id=connection, name=connection, input_node=depnet.arcs[connection].input_node,
                           output_node=depnet.arcs[connection].output_node, frequency=depnet.arcs[connection].frequency)
                cnet.arcs[connection] = arc
            nets[process] = cnet
        return nets

    def calculate_possible_binds(self, process_id, activity_node, log, window_size):
        input_binds = {}
        output_binds = {}
        # select the proper process
        if process_id in log.processes:
            input_arcs = activity_node.input_arcs
            output_arcs = activity_node.output_arcs
            # for each case in the process log check the activity position
            for case in log.processes[process_id].cases:
                candidate_input_binds = set()
                candidate_output_binds = set()
                found = False
                counter = 0
                for event in log.cases[case].events:
                    activity = log.activities[log.activity_instances[log.events[event].activity_instance_id].activity_id]
                    if not found:
                        if activity.name == activity_node.name:
                            found = True
                            counter = 0
                        else:
                            arc_name = activity.name+"->"+activity_node.name
                            if (arc_name in input_arcs) and (counter < window_size):
                                candidate_input_binds.add(activity.name)
                            counter += 1
                    else:
                        arc_name = activity_node.name+"->"+activity.name
                        if (arc_name in output_arcs) and (counter < window_size):
                            candidate_output_binds.add(activity.name)
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
        return input_binds, output_binds

    def mine(self, log, frequency_threshold=0, dependency_threshold=0.0):
        dgraph = self.mine_dependency_graph(log, frequency_threshold=frequency_threshold, dependency_threshold=dependency_threshold)
        cnets = self.mine_cnets(dgraph, log)
        return cnets

    def mine_from_csv_file(self, filename, frequency_threshold=0, dependency_threshold=0.0):
        log_file = CsvLogFactory(input_filename=filename)
        log = log_file.create_log()
        return self.mine(log, frequency_threshold=frequency_threshold, dependency_threshold=dependency_threshold)