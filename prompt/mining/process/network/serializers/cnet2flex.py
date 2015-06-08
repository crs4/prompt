from prompt.mining.process.network.cnet import CNet
import xml.etree.ElementTree as ET
import xml.dom.minidom as MD

class CNet2FlexSerializer():
    """
    Converts a C-Net into its .flex file XML-Based representation
    """
    def __init__(self, cnet):
        self.cnet = cnet
        self.root = ET.Element('cnet')

    def _add_nodes(self):
        for node in self.cnet.nodes:
            label = node.label
            node = ET.SubElement(self.root, 'node', id=label, isInvisible='false')
            node_name = ET.SubElement(node, 'name')
            node_name.text = label

    def _add_bindings(self):
        for node in self.cnet.nodes:
            if not len(node.output_bindings) == 0:
                out_binding = ET.SubElement(self.root, 'outputNode', id = node.label)
                self._add_binding_set(out_binding, node.output_bindings, 'outputSet')
            if not len(node.input_bindings) == 0:
                in_binding = ET.SubElement(self.root, 'inputNode', id = node.label)
                self._add_binding_set(in_binding, node.input_bindings, 'inputSet')

    def _add_start_end_nodes(self):
        start_node = self.cnet.get_initial_nodes()[0]
        end_node = self.cnet.get_final_nodes()[0]
        ET.SubElement(self.root, 'startTaskNode', id=start_node.label)
        ET.SubElement(self.root, 'endTaskNode', id = end_node.label)


    def _add_binding_set(self, bind_element, bindings, bind_tag):
        for b in bindings:
            binding_set = ET.SubElement(bind_element, bind_tag)
            for n in b.node_set:
                ET.SubElement(binding_set, 'node', id=n.label)

    def _add_arcs(self):
        for arc in self.cnet.arcs:
            ET.SubElement(self.root, 'arc', id=arc.label, source=arc.start_node.label, target = arc.end_node.label)

    def serialize(self):
        ET.SubElement(self.root, 'net', type="http://www.processmining.org", id="Causal Net - %s"%self.cnet.label)
        name = ET.SubElement(self.root, 'name')
        name.text = self.cnet.label
        self._add_nodes()
        self._add_start_end_nodes()
        self._add_bindings()
        self._add_arcs()

    def create_flex_file(self, file_dir, is_pretty=False):
        #WARNING: Pretty XML file cannot be Imported into ProM!!!!
        f = open(file_dir, 'w')
        stream = ET.tostring(self.root, 'utf-8')
        if is_pretty:
            reparsed = MD.parseString(stream)
            stream = reparsed.toprettyxml(indent="\t")
        f.write(stream)
        f.close()



class FlexCNetLoader():
    def __init__(self, file_dir):
        self.file_dir= file_dir

    def load(self):
        try:
            cnet = CNet()
            cnet_file = open(self.file_dir)
            f = cnet_file.read()
            root = ET.fromstring(f)
            cnet_file.close()
            net = root.findall('.')[0]
            cnet.label = net.findall('name')[0].text
            #add nodes to the C-Net
            nodes = net.findall('node')
            for node in nodes:
                label = node.attrib['id']
                cnet.add_node(label=label)
            #add output bindings
            output_nodes = net.findall('outputNode')
            for out_node in output_nodes:
                out_node_id = out_node.attrib['id']
                out_cnet = cnet.get_node_by_label(out_node_id)
                out_node_set = out_node.findall('outputSet')
                for out_node_set in out_node_set:
                    nodes = out_node_set.findall('node')
                    binding_set = set()
                    for n in nodes:
                        n_id = n.attrib['id']
                        binding_set.add(cnet.get_node_by_label(n_id))
                    cnet.add_output_binding(out_cnet, binding_set)
            #add input bindings
            input_nodes = net.findall('inputNode')
            for in_node in input_nodes:
                in_node_id = in_node.attrib['id']
                in_cnet = cnet.get_node_by_label(in_node_id)
                in_node_set = in_node.findall('inputSet')
                for in_node in in_node_set:
                    nodes = in_node.findall('node')
                    binding_set = set()
                    for n in nodes:
                        n_id = n.attrib['id']
                        binding_set.add(cnet.get_node_by_label(n_id))
                    cnet.add_input_binding(in_cnet, binding_set)
            #TODO: Check if such control is needed
            arcs = net.findall('arc')
            for a in arcs:
                source = cnet.get_node_by_label(a.attrib['source'])
                target = cnet.get_node_by_label(a.attrib['target'])
                cnet_arc = cnet.get_arc_by_nodes(source, target)
                if not cnet_arc:
                    cnet.add_arc(arc)
            return cnet
        except Exception, e:
            print 'Error while parsing the flex file:'+str(e)




# if __name__ == '__main__':
#         m_net = CNet()
#         a, b, c, d, e, f = m_net.add_nodes('a', 'b', 'c','d', 'e', 'f')
#         m_net.add_output_binding(a, {b,c,d})
#         m_net.add_output_binding(a, {b,c,e})
#         m_net.add_input_binding(b, {a})
#         m_net.add_input_binding(c, {a})
#         m_net.add_input_binding(d, {a})
#         m_net.add_input_binding(e, {a})
#         m_net.add_output_binding(c, {f})
#         m_net.add_output_binding(b, {f})
#         m_net.add_output_binding(d, {f})
#         m_net.add_output_binding(e, {f})
#         m_net.add_input_binding(f, {b,c,e})
#         m_net.add_input_binding(f, {b,c,d})
#         cnet_flex = CNet2FlexSerializer(m_net)
#         cnet_flex.serialize()
#         file_dir = '/Users/ale/Desktop/datasets/flex_test.flex'
#         cnet_flex.create_flex_file(file_dir)
#         loader = FlexCNetLoader(file_dir)
#         cnet = loader.load()
