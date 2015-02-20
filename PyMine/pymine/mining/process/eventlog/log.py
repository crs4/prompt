__author__ = 'paolo'

class Log(object):

    def __init__(self, processes=[]):
        self.processes = processes

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