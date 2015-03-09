from pymine.mining.process.network.cnet import CNet
from pymine.mining.process.network.bpmn import BPMNDiagram


class CNetBPMNConverter(object):
    def __init__(self, cnet):
        self.cnet = cnet

    def convert_to_BPMN(self):
        out_bpmn = BPMNDiagram(label='out')
        #init_cnet_nodes = cnet.get_initial_nodes()
        #add all the initial activities
        cnet = self.cnet
        for node in cnet._nodes:
            out_bpmn.add_node(label=node.label)
        cnet_start = cnet.get_initial_nodes()
        #create all the Network according to C-NET bindings
        elem_out = {}
        elem_in = {}
        for node in cnet._nodes:
            if len(node.output_bindings) == 1:
                for out_binding in node.output_bindings:
                    if len(out_binding.node_set) == 1:
                        elem_out[node] = node
                    else:
                        #add parallel gateway and a transaction collecting activity to the gateway
                        elem_out[node] = out_bpmn.add_node(label=node.label+'gw', node_type='parallel_gateway')
                        out_bpmn.add_arc(out_bpmn.get_node_by_label(node.label), elem_out[node])

            elif len(node.output_bindings) > 1:
                out_bindings = node.output_bindings
                pairwise_disjoint = True
                for i, oset1 in enumerate(out_bindings):
                    for oset2 in out_bindings[i+1:]:
                        if len(oset1.node_set & oset2.node_set) > 0:
                            pairwise_disjoint = False
                            break
                if pairwise_disjoint:
                    elem_out[node] = out_bpmn.add_node(label=node.label+'gw', node_type='exclusive_gateway')
                    out_bpmn.add_arc(out_bpmn.get_node_by_label(node.label), elem_out[node])
                else:
                    elem_out[node] = out_bpmn.add_node(label=node.label+'gw', node_type='inclusive_gateway')
                    out_bpmn.add_arc(out_bpmn.get_node_by_label(node.label), elem_out[node])

            #check all input bindings
            if len(node.input_bindings) == 1:
                for input_binding in node.input_bindings:
                    if len(input_binding.node_set) == 1:
                        elem_in[node] = node
                    else:
                        elem_in[node] = out_bpmn.add_node(label=node.label+'gw', node_type='parallel_gateway')
                        out_bpmn.add_arc(elem_in[node], out_bpmn.get_node_by_label(node.label))
            elif len(node.input_bindings) > 1:
                input_bindings = list(node.input_bindings)
                pairwise_disjoint = True
                for i, oset1 in enumerate(input_bindings):
                    for oset2 in input_bindings[i+1:]:
                        if len(oset1.node_set & oset2.node_set) > 0:
                            pairwise_disjoint = False
                            break
                if pairwise_disjoint:
                    elem_in[node] = out_bpmn.add_node(label=node.label+'gw', node_type ='exclusive_gateway')
                    out_bpmn.add_arc(elem_in[node], out_bpmn.get_node_by_label(node.label))
                else:
                    elem_in[node] = out_bpmn.add_node(label=node.label+'gw', node_type ='inclusive_gateway')
                    out_bpmn.add_arc(elem_in[node], out_bpmn.get_node_by_label(node.label))

        for node in cnet._nodes:
            for oset in node.output_bindings:
                for opp in oset.node_set:
                    out_bpmn.add_arc(elem_out[node], out_bpmn.get_node_by_label(opp.label))

        return out_bpmn



if __name__ == '__main__':
    print 'convert'
    net = CNet()
    a, b, c = net.add_nodes('a', 'b', 'c')
    binding_a_b = net.add_output_binding(a, {b})
    binding_b_c = net.add_output_binding(b, {c})
    binding_b_a = net.add_input_binding(b, {a})
    binding_c_b = net.add_input_binding(c, {b})
    c = CNetBPMNConverter(net)
    bpmn = c.convert_to_BPMN()
    for arc in bpmn.arcs:
        print arc.label
    print bpmn
