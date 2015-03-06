from pymine.mining.process.network.graph import graph_factory
from pymine.mining.process.network import UnexpectedEvent
import logging


GRAPH_IMPL = 'nx'
_default_cost_function = lambda log_move, model_move: 0 if (log_move == model_move) and \
                                                           (log_move.value is not None) else 1


class Alignment(object):
    null_move = '-'

    def __init__(self, log_moves, net_moves, cost):
        self.log_moves = log_moves
        self.net_moves = net_moves
        self.cost = cost

    def __str__(self):
        s = ' '.join(self.get_flat_log_moves())
        s += '\n' + ' '.join(self.get_flat_net_moves())
        return s

    def get_flat_log_moves(self):
        return [m.value if m.value else self.null_move for m in self.log_moves]

    def get_flat_net_moves(self):
        return [m.value if m.value else self.null_move for m in self.net_moves]


class Move(object):
    def __init__(self, value=None):
        self.value = value

    def __repr__(self):
        return str(self.value) if self.value else '-'

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self.value == other.value


def compute_optimal_alignment(case, net, cost_function=None):

    net.rewind() # TODO design a better api for network reset

    class FakeMove(Move):
        pass

    cost_function = cost_function or _default_cost_function

    g = graph_factory(GRAPH_IMPL)
    start = FakeMove('start')
    end = FakeMove('end')

    g.add_node(start)
    g.add_node(end)
    case = [e.activity_name for e in case.events]

    def add_moves_to_graph(log_move, net_move, previous_move):
        logging.debug('-----log_move %s', log_move)
        logging.debug('-----net_move %s', net_move)
        log_move = Move(log_move)
        net_move = Move(net_move)
        g.add_node(log_move)
        g.add_node(net_move)
        g.add_edge(previous_move, log_move, {'cost': 0})
        g.add_edge(log_move, net_move, {'cost': cost_function(log_move, net_move)})

        return log_move, net_move

    def add_move(event_index, net_, previous_move=None):
        available_nodes = net_.available_nodes
        event = case[event_index] if event_index < len(case) else None
        logging.debug('***********add_move************')
        logging.debug('event %s', event)
        logging.debug('available_nodes %s', available_nodes)
        logging.debug('net_.events_played %s', net_.events_played)

        if event is None and available_nodes:
            # start_node = list(available_nodes)[0]
            # logging.debug('***********shortest path in net, start_node %s', start_node)
            # cost, path = net_.shortest_path(start_node)
            # logging.debug('******path %s', path)
            # for node in path:
            #     l_m = None
            #     n_m = node.label
            #     logging.debug('node %s', node)
            #     log_move, net_move = add_moves_to_graph(l_m, n_m, previous_move)
            #     previous_move = net_move
            #
            # g.add_edge(previous_move, end, {'cost': 0})
            logging.debug('DONE')

        elif event:
            available_events = [n.label for n in available_nodes]
            logging.debug('available_events %s', available_events)
            if event in available_events:
                logging.debug('event in available_events')
                try:
                    net_.replay_event(event)
                except UnexpectedEvent:
                    pass
                log_move, net_move = add_moves_to_graph(event, event, previous_move)
                add_move(event_index + 1, net_, net_move)

            else:
                logging.debug('event NOT in available_events')
                l_m = event
                n_m = None
                log_move, net_move = add_moves_to_graph(l_m, n_m, previous_move)
                add_move(event_index + 1, net_, net_move)

                for e in available_events:
                    logging.debug('cycling over all available nodes %s, e: %s, event %s', available_nodes, e, event)
                    l_m = None
                    n_m = e
                    played_events = net_.events_played
                    net_clone = net_.clone()
                    logging.debug('replay_sequence %s', played_events)
                    result, obl, unexpected = net_clone.replay_sequence(played_events)
                    logging.debug('result %s, obl %s, unexpected %s', result, obl, unexpected)
                    logging.debug('---------replaying event %s, available_nodes %s', e, net_clone.available_nodes)

                    net_clone.replay_event(e)

                    log_move, net_move = add_moves_to_graph(l_m, n_m, previous_move)
                    add_move(event_index, net_clone, net_move)

        else:
            g.add_edge(previous_move, end, {'cost': 0})

    add_move(0, net, start)
    optimal_cost, optimal_path = g.shortest_path(start, end, 'cost')
    optimal_path = optimal_path[1: -1]
    log_moves = [m for i, m in enumerate(optimal_path) if not i % 2]
    net_moves = [m for i, m in enumerate(optimal_path) if i % 2]
    optimal_alignment = Alignment(log_moves, net_moves, optimal_cost)
    return optimal_alignment


def case_fitness(case, net, cost_function=None):
    net.rewind()
    cost_function = cost_function or _default_cost_function
    optimal_aln = compute_optimal_alignment(case, net, cost_function)
    cost, shortest_path = net.shortest_path()
    worst_scenario_cost = 0.0
    for event in case.events:
        worst_scenario_cost += cost_function(event, None)
    for node in shortest_path:
        worst_scenario_cost += cost_function(None, node.label)
    logging.debug('optimal_aln.cost %s', optimal_aln.cost)
    logging.debug('worst_scenario_cost %s', worst_scenario_cost )
    return 1 - optimal_aln.cost/worst_scenario_cost


