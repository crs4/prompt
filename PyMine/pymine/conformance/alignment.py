from pymine.mining.process.network.graph import graph_factory
import logging


GRAPH_IMPL = 'nx'


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


def case_fitness(case, net, cost_function=None):
    pass


def compute_optimal_alignment(case, net, cost_function=None):
    class FakeMove(Move):
        pass

    cost_function = cost_function or \
        (lambda log_move, model_move: 0 if (log_move == model_move) and (log_move.value is not None) else 1)

    g = graph_factory(GRAPH_IMPL)
    start = FakeMove('start')
    end = FakeMove('end')

    g.add_node(start)
    g.add_node(end)
    case = [e.activity_name for e in case.events]

    def add_moves_to_graph(log_move, net_move, previous_move):
        logging.debug('log_move %s', log_move)
        logging.debug('net_move %s', net_move)
        log_move = Move(log_move)
        net_move = Move(net_move)
        g.add_node(log_move)
        g.add_node(net_move)
        g.add_edge(previous_move, log_move, {'cost': 0})

        cost = 0 if isinstance(previous_move, FakeMove) else cost_function(previous_move, log_move)
        g.add_edge(log_move, net_move, {'cost': cost_function(log_move, net_move)})

        return log_move, net_move

    def add_move(event_index, available_nodes, previous_move=None):
        event = case[event_index] if event_index < len(case) else None
        logging.debug('event %s', event)
        logging.debug('available_nodes %s', available_nodes)

        if event is None and available_nodes:
            # TODO use shortest path in net
            start_node = list(available_nodes)[0]
            logging.debug('***********shortest path in net, start_node %s', start_node)
            cost, path = net.shortest_path(start_node)
            logging.debug('******path %s', path)
            for node in path:
                l_m = None
                n_m = node.label
                logging.debug('node %s', node)
                log_move, net_move = add_moves_to_graph(l_m, n_m, previous_move)
                previous_move = net_move

            g.add_edge(previous_move, end, {'cost': 0})

        elif event:
            available_events = [n.label for n in available_nodes]
            logging.debug('available_events %s', available_events)
            if event in available_events:
                net.replay_event(event)
                log_move, net_move = add_moves_to_graph(event, event, previous_move)
                next_node = net.get_node_by_label(event)
                add_move(event_index + 1, net.available_nodes, net_move)

            else:
                l_m = event
                n_m = None
                log_move, net_move = add_moves_to_graph(l_m, n_m, previous_move)
                add_move(event_index + 1, net.available_nodes, net_move)

                for e in available_events:
                    l_m = None
                    n_m = e
                    log_move, net_move = add_moves_to_graph(l_m, n_m, previous_move)
                    next_node = net.get_node_by_label(e)
                    add_move(event_index, next_node.output_nodes, net_move)

        else:
            g.add_edge(previous_move, end, {'cost': 0})

    add_move(0, net.available_nodes, start)
    optimal_cost, optimal_path = g.shortest_path(start, end, 'cost')
    optimal_path = optimal_path[1: -1]
    log_moves = [m for i, m in enumerate(optimal_path) if not i % 2]
    net_moves = [m for i, m in enumerate(optimal_path) if i % 2]
    optimal_alignment = Alignment(log_moves, net_moves, optimal_cost)
    return optimal_alignment
