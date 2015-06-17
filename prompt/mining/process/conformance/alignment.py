from prompt.mining.process.network.graph import graph_factory
from prompt.mining.process.network import UnexpectedEvent
from prompt.mining.process.eventlog.log import Log
from prompt.mining.process.eventlog import Case
import logging
from prompt.mining.process.eventlog.log import Classifier
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


def compute_optimal_alignment(case, net, cost_function=None, max_depth=30, classifier=None):

    net.rewind()  # TODO design a better api for network reset

    class FakeMove(Move):
        pass

    cost_function = cost_function or _default_cost_function

    g = graph_factory(GRAPH_IMPL)
    start = FakeMove('start')
    end = FakeMove('end')

    g.add_node(start)
    g.add_node(end)
    case = [e.name for e in case.events]

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

    def add_move(event_index, net_, previous_move=None, depth=0, visited_nodes=None):
        visited_nodes = visited_nodes or set()

        if depth >= max_depth:
            logger.debug('exiting, max depth %s reached', max_depth)
            g.add_edge(previous_move, end, {'cost': 0})
            return

        event = case[event_index] if event_index < len(case) else None
        available_nodes = net_.available_nodes
        available_events = [n.label for n in available_nodes]
        logger.debug('add_move event %s, available_events %s, depth %s', event, available_events, depth)
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
                logger.debug('visited_nodes %s', visited_nodes)
                if net_event not in visited_nodes:
                    visited_nodes.add(net_event)
                    l_m = None
                    n_m = net_event
                    net_clone = net_.clone()
                    logger.debug('replay_sequence %s', played_events)
                    result, obl, unexpected = net_clone.replay_sequence(played_events)
                    net_clone.replay_event(net_event)
                    log_move, net_move = add_moves_to_graph(l_m, n_m, previous_move)
                    add_move(event_index, net_clone, net_move, depth + 1, visited_nodes.copy())

                # if net_event not in played_events:  # this should avoid useless loop
                #     l_m = None
                #     n_m = net_event
                #     net_clone = net_.clone()
                #     logger.debug('replay_sequence %s', played_events)
                #     result, obl, unexpected = net_clone.replay_sequence(played_events)
                #     net_clone.replay_event(net_event)
                #     log_move, net_move = add_moves_to_graph(l_m, n_m, previous_move)
                #     add_move(event_index, net_clone, net_move, depth + 1)
                # else:
                #     g.add_edge(previous_move, end, {'cost': 0})
                #     # does not matter if it is not final node, in case of weird net
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


def _worst_scenario_cost(case, cost_function, shortest_path, classifier):
    worst_scenario_cost = 0.0
    for event in case.events:
        event = classifier.get_event_name(event)
        worst_scenario_cost += cost_function(event, None)
    for node in shortest_path:
        worst_scenario_cost += cost_function(None, node.label)

    return worst_scenario_cost


def _case_fitness(case, net, cost_function, shortest_path, max_depth, classifier):
    optimal_aln = compute_optimal_alignment(case, net, cost_function, max_depth, classifier)
    worst_scenario_cost = _worst_scenario_cost(case, cost_function, shortest_path, classifier)
    return optimal_aln, worst_scenario_cost


def _log_fitness(log, net, cost_function, shortest_path, max_depth, classifier):
    total_cost = 0.0
    total_worst_scenario_cost = 0.0
    for case in log.cases:
        logger.debug('alignment for case %s', [e.name for e in case.events])
        optimal_aln, worst_scenario_cost = _case_fitness(case, net, cost_function, shortest_path, max_depth, classifier)
        total_cost += optimal_aln.cost
        total_worst_scenario_cost += worst_scenario_cost
    return 1 - total_cost/total_worst_scenario_cost


def fitness(events_container, net, cost_function=None, max_depth=30, classifier=None):
    """
    :param events_container: a :class:`prompt.mining.process.eventlog.log.ProcessLog`
        or :class:`prompt.mining.process.eventlog.Case` instance
    :param net: a :class:`prompt.mining.process.network.Network` instance (or a subclass of it)
    :param cost_function: a function with two parameters, log_move and net_move. It should assign a cost to the tuple
     log_move, net_move. Default: 1 if log_move != net_move else 0
    :param max_depth: the max depth of the alignment, used in case of loop. Default: 30
    :return: an integer between 0 an 1 representing the fitness of the events_container on the net
    """
    net.rewind()
    cost_function = cost_function or _default_cost_function
    cost, shortest_path = net.shortest_path()
    classifier = classifier or Classifier()
    if isinstance(events_container, Log):
        return _log_fitness(events_container, net, cost_function, shortest_path, max_depth, classifier)

    elif isinstance(events_container, Case):
        optimal_aln, worst_scenario_cost = _case_fitness(
            events_container, net, cost_function, shortest_path, max_depth, classifier)
        return 1 - optimal_aln.cost/worst_scenario_cost

    else:
        raise Exception('events_container must be ProcessLog or Case instance')
