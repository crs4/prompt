class Log(object):

    def __init__(self, cases=None):
        self._processes = set()
        self.cases = []
        if cases is not None:
            for case in cases:
                self.add_case(case)

    def add_case(self, case):
        self.cases.append(case)
        case.log = self
        self._processes.add(case.process)

    @property
    def processes(self):
        return list(self._processes)

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                for counter in xrange(len(self.processes)):
                    assert self.processes[counter] == other.processes[counter]
            except AssertionError, e:
                return False
            return True
        else:
            return False


class LogInfo(object):

    def __init__(self, log=None, processes_info={}):
        self.log = log
        self.processes_info = processes_info

    def get_process_size(self):
        return len(self.log.processes)

    def get_process_info(self, process_id):
        if process_id in self.processes_info:
            return self.processes_info[process_id]