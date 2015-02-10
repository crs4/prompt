__author__ = 'paolo'

import sys, os
import networkx as nx
import matplotlib.pyplot as plt

from pymine.mining.process.discovery.heuristic import HeuristicMiner as HeuristicMiner

def draw_net_graph(net, output_filename):
    g=nx.MultiDiGraph()
    for node in net.nodes:
        g.add_node(net.nodes[node].name)
    for arc in net.arcs:
        g.add_edge(net.arcs[arc].input_node, net.arcs[arc].output_node, weight=net.arcs[arc].frequency)
    a = nx.to_agraph(g)
    a.layout(prog='dot')
    a.draw(output_filename)

def draw_data_graph(net, output_filename):
    dir, filename = os.path.split(output_filename)
    name, ext = filename.split('.')
    freq_filename = os.path.join(dir, name+'-freq'+ext)
    dep_filename = os.path.join(dir, name+'-dep'+ext)
    frequency_numbers = []
    dependency_numbers = []
    for arc in net.arcs:
        frequency_numbers.append(net.arcs[arc].frequency)
        dependency_numbers.append(net.arcs[arc].dependency)
    print frequency_numbers
    print dependency_numbers

    xmax = len(frequency_numbers)
    ymax = max(frequency_numbers)
    plt.plot(frequency_numbers)
    plt.title("Frequency Histogram")
    plt.xlabel("Arcs")
    plt.ylabel("Frequency")
    plt.axis([0, xmax, 0, ymax])
    plt.savefig(freq_filename)

    xmax = len(dependency_numbers)
    ymax = 1.2
    plt.plot(dependency_numbers)
    plt.title("Dependency Histogram")
    plt.xlabel("Arcs")
    plt.ylabel("Dependency")
    plt.axis([0, xmax, 0, ymax])
    plt.savefig(dep_filename)



def draw_data(net, output_filename):
    dir, filename = os.path.split(output_filename)
    name, ext = filename.split('.')
    data_filename = os.path.join(dir, name+'-data'+ext)
    frequency_numbers = []
    dependency_numbers = []
    for arc in net.arcs:
        frequency_numbers.append(net.arcs[arc].frequency)
        dependency_numbers.append(net.arcs[arc].dependency)
    print frequency_numbers
    print dependency_numbers

    f, axarr = plt.subplots(2, 2)
    axarr[0, 0].plot(frequency_numbers)
    axarr[0, 0].set_title('Frequency')
    axarr[0, 1].plot(dependency_numbers, color='r')
    axarr[0, 1].set_title('Dependency')
    axarr[1, 0].hist(frequency_numbers)
    axarr[1, 0].set_title('Frequency Histogram')
    axarr[1, 1].hist(dependency_numbers, color='r')
    axarr[1, 1].set_title('Dependency Histogram')

    plt.savefig(data_filename)

def main(argv):
    try:
        input_file = argv[1]
        output_file = argv[2]

        miner = HeuristicMiner()
        hnet = miner.mine_from_csv_file(input_file, frequency_threshold=50, dependency_threshold=0.91)
        '''
        print "Network ID: "+str(hnet.id)
        for node in hnet.nodes:
            print "Node: "+str(hnet.nodes[node])
        for arc in hnet.arcs:
            print "Arc: "+str(hnet.arcs[arc])
        '''
        draw_net_graph(hnet, output_file)
        #draw_data_graph(hnet, output_file)
        draw_data(hnet, output_file)
    except Exception, e:
        print "An error occurred: "+str(e)

if __name__ == '__main__':
  main(sys.argv)