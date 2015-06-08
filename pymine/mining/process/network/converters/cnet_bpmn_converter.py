from pymine.mining.process.network.cnet import CNet
from pymine.mining.process.network.cnet import CNet
from pymine.mining.process.network.bpmn import BPMNDiagram
from pymine.mining.process.network.bpmn.utils import BPMNUtils
import logging

class CNetBPMNConverter(object):
    """
    Converts a Causal Network to a BPMN model. Conversion is performed according to these steps:
        1) Identify start and end node, and add START and END BPMN activities accordingly
        2) Convert all nodes of the network to BPMN activities
        3) Convert all output bindings to BPMN split gatweways (XOR/AND, according to the number of binding sets
           and to the number of nodes of each own binding set)
        4) Convert all input bindings to BPMN join gateways (XOR/AND, XOR/AND, according to the number of binding sets
           and to the number of nodes of each own binding set)
        5) Gateways reduction, using BPMN utils
        6) Gateways and activities merge, using BPMN utils
    """

    def __init__(self, cnet):
        self.cnet = cnet
        self.start_act = None
        self.end_act = None
        self.activity_map = {}
        self.arc_nodes_out_map = {}
        self.arc_nodes_in_map = {}
        self.conversion_map = {}
        self.EXCLUSIVE = 'exclusive_gateway'
        self.PARALLEL = 'parallel_gateway'

    def convert_to_BPMN(self):
        """
        Convert a C-NET to a BPMN object.

        :return: The converted :class: `.BPMNDiagram` object
        """
        bpmn = BPMNDiagram(label='bpmn')
        self.check_start_activity_exists(self.cnet.nodes)
        self.check_end_activity_exists(self.cnet.nodes)
        self.convert_activities(bpmn)
        self.convert_output_bindings(bpmn)
        self.convert_input_bindings(bpmn)
        self.add_arc_flows(bpmn)
        self.remove_synthetic_start_and_end_activities(bpmn)
        util = BPMNUtils()
        util.reduce_gateways(bpmn)
        util.merge_activities_and_gateways(bpmn)
        return bpmn

    def check_start_activity_exists(self, activities):
        """
        Checks if a start activity exists in the C-NET, and if yes, assigns it to the `start_act` parameter.

        :param activities: A list containing all C-Net nodes (activities)
        :raises Exception if no start activity has been found
        """
        if not activities:
            logging.debug('Null activities object')
            return
        for act in activities:
            if not act.input_nodes:
                self.start_act = act
                return
        raise Exception('No start activity in the C-net')

    def check_end_activity_exists(self, activities):
        """
        Checks if an end  activity exists in the C-NET, and if yes, assigns it to the `start_act` parameter.

        :param activities: A list containing all C-Net nodes (activities)
        :raises:Exception if no end activity has been found
        """
        if not activities:
            logging.debug('Null end activities object')
            return
        for act in activities:
            if not act.output_nodes:
                self.end_act = act
                return
        raise Exception('No end activity in the C-net')

    def convert_activities(self, bpmn):
        """
        Converts all C-NET activities (Nodes) to BPMN activities. For each node, in the C-NET, a node is created in the
        BPMN. For start and end activities, two additional START and END nodes are created in the BPMN network, and these
        nodes are connected to the start and end activities respectively.

        :param bpmn: The BPMN network object
        """
        for node in self.cnet.nodes:
            act = bpmn.add_node(label=node.label)
            self.activity_map[node] = act
            self.conversion_map[node.label] = act
            if node == self.start_act:
                start = bpmn.add_node(label='START')
                bpmn.add_arc(start, act)
            if node == self.end_act:
                end = bpmn.add_node(label='END')
                bpmn.add_arc(act, end)

    def get_outputs_with_target(self, target, source):
        """
        Retrieves all output bindings of the given source node containing the target node.

        :param target: The target node
        :param source:  The source node
        :return: A list containing the output bindings satisfying the condition described above
        """
        result = []
        for output in source.output_bindings:
            if output.node_set.__contains__(target):
                result.append(output)
        return result

    def get_inputs_with_source(self, source, node):
        """
        Retrieves all input bindings of the given source node containing the target node.

        :param target: The target node
        :param source:  The source node
        :return: A list containing the input bindings satisfying the condition described above
        """
        result = []
        for input in node.input_bindings:
            if input.node_set.__contains__(source):
                result.append(input)
        return result


    def convert_output_bindings(self, bpmn):
        """
        Converts all output bindings of the C-NET to BPMN gateway split nodes, according to the following rules:
            1) If the output binding of the current node is composed of more than one binding set, an XOR gateway
               is created, and the current node is connected to the gateway
            2) For each binding set:
                    2.1) If the binding set is composed of more than one node, an AND gateway is created for each of the
                       nodes belonging to the binding set. All created gateways are then collected to the current node

                    2.2) Retrieve all end nodes of all arcs outcoming from the current node, and check whether an end node
                         is contained in more than one output binding set of the current node. If yes, create an XOR
                         gateway and connect it to each node of the output binding set

        :param bpmn: The BPMN network object
        """
        for node in self.activity_map.keys():
            out_bindings = node.output_bindings
            current_bpmn_act = self.activity_map[node]
            if len(out_bindings) > 1:
                #create an XOR gateway (Exclusive)
                xor_gw = bpmn.add_node(label=(node.label+'_O'), node_type=self.EXCLUSIVE)
                bpmn.add_arc(current_bpmn_act, xor_gw)
                current_bpmn_act = xor_gw
            output_counter = 0
            outputs_map = {}
            for out in out_bindings:
                bpmn_output = current_bpmn_act
                if len(out.node_set) > 1:
                    #in this case we have to create an AND gateway, as the binding set contains more than one node
                    and_gw = bpmn.add_node(label=(node.label+'_O'+str(output_counter)), node_type=self.PARALLEL)
                    bpmn.add_arc(current_bpmn_act, and_gw)
                    bpmn_output = and_gw
                output_counter += 1
                outputs_map[out] = bpmn_output
            outgoing_arcs = node.output_arcs
            for a in outgoing_arcs:
                out_with_targets = self.get_outputs_with_target(a.end_node, node)
                if len(out_with_targets) > 1:
                    #add XOR gateway
                    arc_gw = bpmn.add_node(label=(node.label+'_'+a.end_node.label), node_type=self.EXCLUSIVE)
                    for out_target in out_with_targets:
                        bpmn.add_arc(outputs_map[out_target], arc_gw)
                    self.arc_nodes_out_map[a] = arc_gw
                elif len(out_with_targets) == 1:
                    arc_node = outputs_map[out_with_targets[0]]
                    self.arc_nodes_out_map[a] = arc_node

    def convert_input_bindings(self, bpmn):
        """
        Converts all input bindings of the C-NET, according to the following rules:
            1) If the input binding of the current node is composed of more than one binding set, a join XOR gateway
               is created, and the current node is connected to the gateway
            2) For each input binding:
                2.1) If the node set of the binding is composed of more than one node, an AND join gateway is created
               and the current node is connected to the gateway
               2.2) All the input arcs of the current node are retrived, and for each source node of these input arc
                    it is checked if the source node is contained in more than one input binding set. if yes, an
                    additional XOR join gateway is created, and each node satisfying teh condition is connected to the
                    join gateway
        :param bpmn:
        :return:
        """
        for node in self.activity_map.keys():
            in_bindings = node.input_bindings
            current_bpmn_act = self.activity_map[node]
            if len(in_bindings) > 1:
                #Create input XOR Join
                xor_gw = bpmn.add_node(label=(node.label+'I'), node_type=self.EXCLUSIVE)
                bpmn.add_arc(xor_gw, current_bpmn_act)
                current_bpmn_act = xor_gw
            input_counter = 0
            inputs_map = {}
            for input in in_bindings:
                bpmn_input = current_bpmn_act
                if len(input.node_set) > 1:
                    #Create input AND join, as the bionding size is >1
                    and_gw = bpmn.add_node(label=(node.label+'_I'+str(input_counter)), node_type=self.PARALLEL)
                    bpmn.add_arc(and_gw, current_bpmn_act)
                    bpmn_input = and_gw
                input_counter += 1
                inputs_map[input] = bpmn_input
            ingoing_arcs = node.input_arcs
            for a in ingoing_arcs:
                inputs_with_source = self.get_inputs_with_source(a.start_node, node)
                if len(inputs_with_source) > 1:
                    #create join XOR
                    arc_gw = bpmn.add_node(label=(a.start_node.label+'_'+node.label), node_type=self.EXCLUSIVE)
                    for input in inputs_with_source:
                        bpmn.add_arc(arc_gw, inputs_map[input])
                    self.arc_nodes_in_map[a] = arc_gw
                elif len(inputs_with_source) == 1:
                    arc_node = inputs_map[inputs_with_source[0]]
                    self.arc_nodes_in_map[a] = arc_node

    def add_arc_flows(self, bpmn):
        """
        Creates the BPMN arcs
        :param The BPMN network object:
        :return:
        """
        for a in self.cnet.arcs:
            if self.arc_nodes_in_map.has_key(a) and self.arc_nodes_out_map.has_key(a):
                bpmn.add_arc(self.arc_nodes_out_map[a], self.arc_nodes_in_map[a])


    def remove_synthetic_activity(self, act, in_arc, out_arc, bpmn ):
        """
        Remove an activity from the BPMN, and connect directly predecessor and successor nodes
        :param act: The activity to remove
        :param in_arc: The activity incoming arc
        :param out_arc: The actovity outgoing arc
        :param bpmn: The BPMN network object
        :return:
        """
        if (not in_arc) or (not out_arc):
            return
        predecessor_act = in_arc.start_node
        successor_act  = out_arc.end_node
        bpmn.remove_arc(in_arc)
        bpmn.remove_arc(out_arc)
        bpmn.add_arc(predecessor_act, successor_act)
        bpmn.remove_activity(act)

    def remove_synthetic_start_and_end_activities(self, bpmn):
        """
        Remove from BPMN the additional start and end activity and connect directly the start and end node to the
        first and the start activity of the BPMN
        :param bpmn: The BPMN network object
        :return:
        """
        start = self.conversion_map[self.start_act.label]
        if not "START" == start.label:
            logging.debug('Remove_synt:No start act')
            return
        end = self.conversion_map[self.end_act.label]
        if not "END" == end.label:
            logging.debug('Remove_synt:No end act')
            return
        start_in, start_out, end_in, end_out = None
        for arc in bpmn.arcs:
            if arc.start_node == start:
                start_out = arc
            elif arc.end_node == start:
                start_in = arc
            elif arc.start_node == end:
                end_out = arc
            elif arc.end_node == end:
                end_in = arc
        self.remove_synthetic_activity(start, start_in, start_out, bpmn)
        self.remove_synthetic_activity(end, end_in, end_out, bpmn)



