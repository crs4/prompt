from pymine.mining.process.network.graph import graph_factory
from pymine.mining.process.network import UnexpectedEvent
from pymine.mining.process.eventlog.log import Log
from pymine.mining.process.eventlog import Case
import logging
logger = logging.getLogger('alignment')

GRAPH_IMPL = 'nx'


def _default_cost_function(log_move, model_move):
    return 0 if (log_move == model_move) and (log_move.value is not None) else 1


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


def compute_optimal_alignment(case, net, cost_function=None, max_depth=30):

    net.rewind()  # TODO design a better api for network reset

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
        logger.debug('-----log_move %s', log_move)
        logger.debug('-----net_move %s', net_move)
        log_move = Move(log_move)
        net_move = Move(net_move)
        g.add_node(log_move)
        g.add_node(net_move)
        g.add_edge(previous_move, log_move, {'cost': 0})
        g.add_edge(log_move, net_move, {'cost': cost_function(log_move, net_move)})

        return log_move, net_move

    def add_move(event_index, net_, previous_move=None, depth=0):
        if depth > max_depth:
            return

        event = case[event_index] if event_index < len(case) else None
        available_nodes = net_.available_nodes
        available_events = [n.label for n in available_nodes]
        logger.debug('add_move event %s, available_events %s', event, available_events)
        if event is None and not available_events:
            g.add_edge(previous_move, end, {'cost': 0})
            logger.debug('final node reached')

        elif event in available_events:
            logger.debug('event in available_events')
            net_.replay_event(event)
            log_move, net_move = add_moves_to_graph(event, event, previous_move)
            add_move(event_index + 1, net_, net_move, depth + 1)
        elif event is None:
            logger.debug('event is None')
            played_events = net_.events_played
            for net_event in available_events:
                l_m = None
                n_m = net_event
                net_clone = net_.clone()
                logger.debug('replay_sequence %s', played_events)
                result, obl, unexpected = net_clone.replay_sequence(played_events)
                net_clone.replay_event(net_event)
                log_move, net_move = add_moves_to_graph(l_m, n_m, previous_move)
                add_move(event_index, net_clone, net_move, depth + 1)
        else:
            logger.debug('else...')
            logger.debug('event %s, available_events %s', event, available_events)
            l_m = event
            n_m = None
            log_move, net_move = add_moves_to_graph(l_m, n_m, previous_move)
            add_move(event_index + 1, net_, net_move, depth + 1)

            played_events = net_.events_played
            logger.debug('played_events %s, available_events %s', played_events, available_events)
            for net_event in set(available_events) - set(played_events):
                l_m = None
                n_m = net_event
                net_clone = net_.clone()
                logger.debug('replay_sequence %s', played_events)
                result, obl, unexpected = net_clone.replay_sequence(played_events)
                net_clone.replay_event(net_event)
                log_move, net_move = add_moves_to_graph(l_m, n_m, previous_move)
                add_move(event_index, net_clone, net_move, depth + 1)

    add_move(0, net, start)
    optimal_cost, optimal_path = g.shortest_path(start, end, 'cost')
    optimal_path = optimal_path[1: -1]
    log_moves = [m for i, m in enumerate(optimal_path) if not i % 2]
    net_moves = [m for i, m in enumerate(optimal_path) if i % 2]
    optimal_alignment = Alignment(log_moves, net_moves, optimal_cost)
    return optimal_alignment


def _worst_scenario_cost(case, cost_function, shortest_path):
    worst_scenario_cost = 0.0
    for event in case.events:
        worst_scenario_cost += cost_function(event, None)
    for node in shortest_path:
        worst_scenario_cost += cost_function(None, node.label)

    return worst_scenario_cost


def _case_fitness(case, net, cost_function, shortest_path, max_depth):
    optimal_aln = compute_optimal_alignment(case, net, cost_function, max_depth)
    worst_scenario_cost = _worst_scenario_cost(case, cost_function, shortest_path)
    return optimal_aln, worst_scenario_cost


def _log_fitness(log, net, cost_function, shortest_path, max_depth):
    total_cost = 0.0
    total_worst_scenario_cost = 0.0
    for case in log.cases:
        optimal_aln, worst_scenario_cost = _case_fitness(case, net, cost_function, shortest_path, max_depth)
        total_cost += optimal_aln.cost
        total_worst_scenario_cost += worst_scenario_cost
    return 1 - total_cost/total_worst_scenario_cost


def fitness(events_container, net, cost_function=None, max_depth=30):
    """
    :param events_container: a Log or Case instance
    :param net: a net
    :param cost_function: a function with two parameters, log_move and net_move. It should assign a cost to the tuple
     log_move, net_move. Default: 1 if log_move != net_move else 0
    :param max_depth: the max depth of the alignment, used in case of loop. Default: 30
    :return: fitness of the events_container on the net
    """
    net.rewind()
    cost_function = cost_function or _default_cost_function
    cost, shortest_path = net.shortest_path()
    if isinstance(events_container, Log):
        return _log_fitness(events_container, net, cost_function, shortest_path, max_depth)

    elif isinstance(events_container, Case):
        optimal_aln, worst_scenario_cost = _case_fitness(events_container, net, cost_function, shortest_path, max_depth)
        return 1 - optimal_aln.cost/worst_scenario_cost

    else:
        raise Exception('events_container must be Log or Case instance')
