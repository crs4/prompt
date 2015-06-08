from prompt.mining.process.eventlog.log import Classifier


def replay_case(case, net, classifier=None):
    """
    :param case: a :class:`prompt.mining.process.eventlog.Case` instance
    :param net: a :class:`prompt.mining.process.network.Network` instance (or a subclass of it)
    :return: a tuple containing: a boolean telling if the replay has been completed successfully,
        a list of obligations and a list of unexpected events.
    """
    classifier = classifier or Classifier()
    return net.replay_sequence([classifier.get_event_name(event) for event in case.events])


def simple_fitness(process_log, net, classifier=None):
    """
    Compute the fitness on the given log and net as the fraction of case replayed successfully to the cardinality of log

    :param process_log: a :class:`ProcessLog <prompt.mining.process.eventlog.log.ProcessLog>` instance
    :param net: a :class:`prompt.mining.process.network.Network` instance (or a subclass of it)
    :return: integer between 0 and 1
    """
    result = FitnessResult(process_log, net)
    total_cases = 0
    success = 0.0
    for case in process_log.cases:
        total_cases += 1
        result_case = replay_case(case, net, classifier)
        if result_case[0]:
            success += 1
        # result.add_replay_result(case, result_case)
    # return result
    result._fitness = success/total_cases
    return result


class FitnessResult(object):
    def __init__(self, log, net):
        self.log = log
        self.net = net
        self.results = {}
        self.failed_cases = []
        self.correct_cases = []
        self._fitness = None

    def add_replay_result(self, case, replay_results):
        self.results[case] = replay_results
        result, obligations, unknown = replay_results
        if result:
            self.correct_cases.append(case)

        else:
            self.failed_cases.append(case)

    @property
    def fitness(self):
        if not self._fitness:

            if self.results:
                n_correct_cases = float(len(self.correct_cases))
                n_failed_cases = float(len(self.failed_cases))
                self._fitness = n_correct_cases/(n_correct_cases + n_failed_cases)
        return self._fitness