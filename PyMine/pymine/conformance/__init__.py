def replay_case(case, net):
    return net.replay_sequence([event.activity_name for event in case.events])


def simple_fitness(log, net):
    result = FitnessResult(log, net)
    for case in log.cases:
        result_case = replay_case(case, net)
        result.add_replay_result(case, result_case)

    return result


class FitnessResult(object):
    def __init__(self, log, net):
        self.log = log
        self.net = net
        self.results = {}
        self.failed_cases = []
        self.correct_cases = []

    def add_replay_result(self, case, replay_results):
        self.results[case] = replay_results
        result, obligations, unknown = replay_results
        if result:
            self.correct_cases.append(case)

        else:
            self.failed_cases.append(case)

    @property
    def fitness(self):
        if self.results:
            n_correct_cases = float(len(self.correct_cases))
            n_failed_cases = float(len(self.failed_cases))
            return n_correct_cases/(n_correct_cases + n_failed_cases)
