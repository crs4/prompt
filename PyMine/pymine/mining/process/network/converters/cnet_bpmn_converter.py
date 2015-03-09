from pymine.mining.process.network.cnet import CNet
from pymine.mining.process.network.cnet import CNet
from pymine.mining.process.network.bpmn import BPMNDiagram
import logging

class CNetBPMNConverter(object):




    def __init__(self, cnet):
        self.cnet = cnet
        self.bpmn_settings = None
        self.EXCLUSIVE_GATEWAY = 'exclusive_gateway'
        self.INCLUSIVE_GATEWAY = 'inclusive_gateway'
        self.PARALLEL_GATEWAY  = 'parallel_gateway'
        self.JOIN = 'JOIN'
        self.SPLIT = ' SPLIT'
        self.PATTERN_TYPE_AND = 'AND'
        self.PATTERN_TYPE_XOR = 'XOR'
        self.PATTERN_TYPE_OR = 'OR'


    def get_output_arcs(self, target):
        """
        Gets the list of all arcs splitting from the target node
        :param target:
        :return:
        """
        return target.output_arcs


    def get_input_arcs(self, target):
        """
        gets the list of all arcs joining in the target node
        :param target:
        :return:
        """
        return target.input_arcs


    def get_nodes_list(self, node, is_out):
        """
        Given a node in the CNET, return a list containing all the incoming nodes(if is_out=False), or all the outcoming nodes (if is_out=True)
        :param node:
        :param is_out:
        :return:
        """
        if is_out:
            return node.output_nodes
        else:
            return node.input_nodes


    def has_XOR(self, binding_node_set):
        """
        Evaluate is a node set binding will generate a XOR in the conversion BPMN network. The evaluation is made checking if all sets are distinct: if it is,
        returns true, as we have an XOR, otherwise we have an OR and returns false
        :param binding_node_set:
        :return:
        """
        if len(binding_node_set) is not 0:
            logging.debug('Checking XOR for node:', binding_node_set[0].node)
            logging.debug('Binding:', [b.node_set for b in binding_node_set])
        has_xor = False
        for b in binding_node_set:
            for b2 in binding_node_set:
                temp_b = b.node_set.copy()
                temp_b2 = b2.node_set.copy()
                if not temp_b == temp_b2:
                    temp_b = temp_b.intersection(temp_b2)
                    if len(temp_b) == 0:
                        continue #the sets have no intersection
                    else:
                        has_xor = True
                        break
            if has_xor:
                break
        logging.debug('XOR presence:', not has_xor)
        return not has_xor





    def convert_all_activities(self, bpmn, id2node, cnet):
        """
        Adds all CNET nodes as activities of the BPMN Netowork, then saves alla activities in a dictionary
        :param bpmn:
        :param id2node:
        :param cnet:
        :return:
        """
        for node in cnet.nodes:
            act = bpmn.add_node(label=node.label)
            id2node[act] = act



    #def create_connect_start_event(self, cnet, bpmn, id2node):
    #    cnet_start_node = cnet.get_initial_nodes()[0]


    #def create_connect_end_event(self, cnet, bpmn, id2node):
    #    pass

    def identify_pattern_type(self, split_type, node):
        """
        According to the pattern type SPLIT/JOIN of the current node, this method returns the current operator to be used in the BPMN network
        :param split_type:
        :param node:
        :return:
        """
        if split_type == self.JOIN:
            pattern_nodes_set = node.input_bindings
        else:
            pattern_nodes_set =  node.output_bindings
        pattern_size = len(pattern_nodes_set)
        logging.debug('Pattern size for node'+node.label+'is:%s'%pattern_size)
        if pattern_size == 1:
            return self.PATTERN_TYPE_AND
        elif self.has_XOR(pattern_nodes_set):
            return self.PATTERN_TYPE_XOR
        else:
            return self.PATTERN_TYPE_OR


    def fill_AND_OR_split_gateway(self, cnet, node, gw_type, id2node, bpmn):
        """
        Builds the split gateway identified by an AND_OR gateway
        :param cnet:
        :param node:
        :param PARALLEL:
        :param id2node:
        :param bpmn:
        :return:
        """
        nodes_list = self.get_nodes_list(node, True)
        gw_id = '' + node.label
        for n in nodes_list:
            gw_id += n.label
        #create BPMN gateway, according to the gateway type
        gw = bpmn.add_node(label=gw_id, node_type=gw_type)
        #connect source node to the gateway and then the gateway to all output node sets
        bpmn.add_arc(bpmn.get_node_by_label(node.label), gw)
        for n in nodes_list:
            bpmn.add_arc(gw, bpmn.get_node_by_label(n.label))



    def fill_XOR_split_gateway(self, bpmn, id2node, cnet, node):
        """
        Builds the split gateways identified to be of XOR type, according to the binding and identifying the follwing cases:
            - If one of the  sets of the binding has two or more nodes (i.e. {A, B}), to represent this we need to add an XOR gateway, then one
              AND gateway for each distinct set of the binding having at least two nodes
            - If noone of the set of the binding has two or more nodes, wi will just create an XOR gateway
        :param bpmn:
        :param id2node:
        :param cnet:
        :param node:
        :return:
        """

        #Create exclusive gateway
        #Create ID for exclusive gateway
        logging.debug('Starting XOR split gateway creation for node:', node.label)
        excl_gw_id = ''+node.label
        out_nodes_list = self.get_nodes_list(node, True)
        logging.debug('Output nodes list:', out_nodes_list)
        for n in out_nodes_list:
            excl_gw_id += n.label
        logging.debug('Created gateway with ID:', excl_gw_id)
        excl_gw = bpmn.add_node(label = excl_gw_id, node_type = self.EXCLUSIVE_GATEWAY)

        #connect entry activity to gateway
        bpmn.add_arc(bpmn.get_node_by_label(node.label), excl_gw)
        out_bind_set = node.output_bindings
        logging.debug('Checking node output binding:',[b.node_set for b in out_bind_set])
        for nset in out_bind_set:
            logging.debug('Current binding node set:', nset.node_set)
            if len(nset.node_set) == 1:
                #simply create transition to connect the node in output set to the gateway
                bpmn.add_arc(excl_gw, bpmn.get_node_by_label(list(nset.node_set)[0].label))
            else:
                logging.debug('Outset composed of more than one node: creating AND+XOR gateways....')
                #More Than one node in outputset. Steps to follow:
                    #1. Create AND Gateway
                    #2. Create new transition to connect AND gateway to XOR gateway
                    #3. Create new transitions to connect nodes in outputset to the AND gateway
                #create and gateway
                #create ID for the gateway
                and_gw_id = "" + excl_gw_id
                for n in nset.node_set:
                    and_gw_id += n.label
                logging.debug('AND gateway id is:', and_gw_id)
                and_gw = bpmn.add_node(label=and_gw_id, node_type=self.PARALLEL_GATEWAY)
                #create new transition to connect AND gateway to XOR gateway
                bpmn.add_arc(excl_gw, and_gw)
                #for each node of the binding, connect the output set node to the AND gateway
                for out_node in nset.node_set:
                    bpmn.add_arc(and_gw, bpmn.get_node_by_label(out_node.label))




    def connect_sequences_and_split_gateways(self, bpmn, bpmn_settings, id2node, cnet):
        """
        Analyze the CNET in order to decide which kind of gateways we need, according to the different bindings
        :param bpmn:
        :param bpmn_settings:
        :param id2node:
        :param cnet:
        :return:
        """
        for node in self.cnet.nodes:
            #First, check how many output arcs are departing from that node
            out_arcs = self.get_output_arcs(node)
            logging.debug('Checking current node for gateway split:'+node.label)
            if len(out_arcs) < 1:
                logging.debug('Current node' +node.label+'has no output arcs')
                continue #node does not have any output arcs (END node )
            elif len(out_arcs) == 1 : #make a connection with the destination node of the output arc
                logging.debug('Current node' +node.label+'has a single output arc:'+out_arcs[0].end_node.label)
                dest_node = out_arcs[0].end_node
                bpmn.add_arc(bpmn.get_node_by_label(node.label), bpmn.get_node_by_label(dest_node.label))
            else: #evaluate the right gateway to add, according to the pattern of the split of the node; it will depend itself from its output bindings
                logging.debug('Current node' +node.label+'has more than 1 output arcs. Identify pattern.....')
                pattern_type = self.identify_pattern_type(self.SPLIT, node)
                if pattern_type == self.PATTERN_TYPE_AND:
                    self.fill_AND_OR_split_gateway(self.cnet, node, self.PARALLEL_GATEWAY, id2node, bpmn)
                elif pattern_type == self.PATTERN_TYPE_XOR:
                    self.fill_XOR_split_gateway(bpmn, id2node, cnet, node)
                else:
                    self.fill_AND_OR_split_gateway(self.cnet, node, self.INCLUSIVE_GATEWAY, id2node, bpmn)



    def fill_join_gateway(self,cnet, bpmn_settings, node, gw_type, bpmn ):
        """
        Creates the Gateways according to the Joins then rempves old connections between activities, replacing them with the Join gateway
        :param cnet:
        :param bpmn_settings:
        :param node:
        :param gw_type:
        :param bpmn:
        :return:
        """
        logging.debug('Joining gateway for node:', node.label)
        #create the Join gateway, according to its type
        id_node_gw = '' + node.label
        source_nodes = self.get_nodes_list(node, False)
        for n in source_nodes:
            id_node_gw += n.label
        #create gw
        gw = bpmn.add_node(label=id_node_gw, node_type=gw_type)
        #Remove incoming transitions, put a gateway between and then connect transitions again
        logging.debug('Source nodes for current node are:', [source.label for source in source_nodes])
        for n in source_nodes:
            logging.debug('checking source node:', n.label)
            #if there is XOR/AND/OR split gateway, we need to remove the old transition
            out_arcs = self.get_output_arcs(n)
            logging.debug('Current node has the following output arcs:')
            for arc in out_arcs:
                logging.debug('Output arc:'+ arc.start_node.label + '----->'+ arc.end_node.label)
            if len(out_arcs) > 1:
                pattern_type = self.identify_pattern_type(self.SPLIT, n)
                logging.debug('Node has more than one output arc. Pattern is:', pattern_type)
                #get output nodes
                out_nodes = self.get_nodes_list(n, True)
                logging.debug('Output nodes for current source node are:', [out.label for out in out_nodes])
                gw_temp_id = ''+n.label
                for n in out_nodes:
                    gw_temp_id += n.label
                logging.debug('Joining gateway........')
                if pattern_type == self.PATTERN_TYPE_AND or pattern_type == self.PATTERN_TYPE_OR:
                    self.remove_old_and_connect_new_transitions_to_gateway(node, id_node_gw, gw_temp_id, bpmn)
                elif pattern_type == self.PATTERN_TYPE_XOR:
                    self.remove_old_and_connect_new_transitions_to_XOR_gateway(node, bpmn, id_node_gw, n, gw_temp_id)
            else:
                #just remove the transition
                self.remove_old_and_connect_new_transitions_to_gateway(node, id_node_gw, n.label, bpmn)

        #final, create new transition connecting the gateway to the index element
        from_node = bpmn.get_node_by_label(id_node_gw)
        to_node = bpmn.get_node_by_label(node.label)
        bpmn.add_arc(from_node, to_node)


    def remove_old_and_connect_new_transitions_to_gateway(self, node, id_node_gw, gw_temp_id, bpmn):
        from_node = bpmn.get_node_by_label(gw_temp_id)
        to_node = bpmn.get_node_by_label(node.label)
        logging.debug('Removing transition from node:', from_node.label, ' to node: ', to_node.label)
        arc_to_remove = bpmn.get_arc_by_nodes(from_node, to_node)
        bpmn.remove_arc(arc_to_remove)
        from_node_2 = bpmn.get_node_by_label(gw_temp_id)
        to_node_2 = bpmn.get_node_by_label(id_node_gw)
        bpmn.add_arc(from_node_2, to_node_2)

    def remove_old_and_connect_new_transitions_to_XOR_gateway(self, node, bpmn, id_node_gw, n, gw_temp_id ):
        output_nodes_bind = node.output_bindings
        for bind in output_nodes_bind:
            output_nodes = bind.node_set
            if len(output_nodes) == 1 and list(output_nodes)[0] == node:
                self.remove_old_and_connect_new_transitions_to_gateway(node, id_node_gw, gw_temp_id, bpmn)
            elif len(output_nodes) >=2 and node in output_nodes:
                #we have more than 2 nodes in the outputset
                #1. Create AND gateway
                #2. Create new transition to connect AND gateway to XOR gateway
                #3. Create new transitions to connect nodes in outputset and the gateway
                id2 = ''+gw_temp_id
                for n in output_nodes:
                    id2+= n.label
                self.remove_old_and_connect_new_transitions_to_gateway(node, id_node_gw, id2, bpmn)


    def connect_join_gateways(self, bpmn, bpmn_settings, id2node, cnet):
        """
        This metor removes all transitions in which join exists, then creates the gateway, and makes connection again
        :param bpmn:
        :param bpmn_settings:
        :param id2node:
        :param cnet:
        :return:
        """
        for node in cnet._nodes:
            input_edges_list = self.get_input_arcs(node)
            if len(input_edges_list) > 1:
                pattern_type = self.identify_pattern_type(self.JOIN, node)
                if pattern_type == self.PATTERN_TYPE_AND:
                    self.fill_join_gateway(self.cnet, bpmn_settings, node, self.PARALLEL_GATEWAY,bpmn)
                elif pattern_type == self.PATTERN_TYPE_XOR:
                    self.fill_join_gateway(self.cnet, bpmn_settings, node, self.EXCLUSIVE_GATEWAY,bpmn)
                elif pattern_type == self.PATTERN_TYPE_OR:
                    self.fill_join_gateway(self.cnet, bpmn_settings, node, self.INCLUSIVE_GATEWAY,bpmn)


    def convert_to_BPMN(self):
        out_bpmn = BPMNDiagram(label='out')
        id2node = {}
        self.convert_all_activities(out_bpmn, id2node, self.cnet)
    #    create_connect_start_event(self.cnet, bpmn, id2node)
    #    create_connect_start_event(self.cnet, bpmn, id2node)
        self.connect_sequences_and_split_gateways(out_bpmn,self.bpmn_settings, id2node, self.cnet)
        self.connect_join_gateways(out_bpmn, self.bpmn_settings, id2node,self.cnet)
        return out_bpmn


