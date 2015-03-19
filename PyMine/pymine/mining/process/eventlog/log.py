from pymine.mining.process.eventlog.exceptions import InvalidProcess
from abc import ABCMeta, abstractmethod, abstractproperty


class BaseLog(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def cases(self):
        pass

    @abstractmethod
    def add_case(self, case):
        pass


class ProcessLog(BaseLog):
    """
    A contaneir for :class:`Case<pymine.mining.process.eventlog.Case>`.They must belong to the same process.
    """
    def __init__(self, process, cases=None):
        self._process = process
        self._cases = []
        if cases is not None:
            for case in cases:
                self.add_case(case)

    @property
    def process(self):
        return self._process

    @property
    def cases(self):
        return self._cases

    def add_case(self, case):
        """
        :param case: a :class:`Case<pymine.mining.process.eventlog.Case>` instance
        :return:
        :raises: :class:`pymine.mining.process.eventlog.exceptions.InvalidProcess`
        """
        if case.process != self.process:
            raise InvalidProcess("cases must belong to process %s, found %s instead" % (self.process, case.process))

        self._cases.append(case)
        case.log = self  # TODO remove this assignment, I mean why a case should belong to a single log instance?


class Log(BaseLog):
    """
    A generic contaneir for :class:`Case<pymine.mining.process.eventlog.Case>`.They can belong to different process.
    To obtain the cases for a specific process (i.e. the relative
    :class:`<ProcessLog>pymine.mining.process.eventlog.log.ProcessLog`) just use the Log as dictionary (log[process]).
    """

    def __init__(self, cases=None, process_logs=()):
        self._process_logs = {}
        for p_log in process_logs:
            self._process_logs[p_log.process] = p_log

        if cases is not None:
            for case in cases:
                self.add_case(case)

    def add_case(self, case):
        """
        :param case: a :class:`Case<pymine.mining.process.eventlog.Case>` instance
        :return:
        """
        if case.process not in self._process_logs:
            self._process_logs[case.process] = ProcessLog(case.process, [case])
        else:
            self._process_logs[case.process].add_case(case)

    @property
    def processes(self):
        return self._process_logs.keys()

    @property
    def cases(self):
        cases = []
        for process_log in self._process_logs.values():
            cases.extend(process_log.cases)
        return cases

    def __getitem__(self, item):
        return self._process_logs[item]

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