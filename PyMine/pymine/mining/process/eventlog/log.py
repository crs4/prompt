from pymine.mining.process.eventlog.exceptions import InvalidProcess
from abc import ABCMeta, abstractmethod, abstractproperty
from collections import OrderedDict


class BaseLog(object):
    __metaclass__ = ABCMeta

    def __init__(self, filename=None):
        self.filename = filename

    @abstractproperty
    def cases(self):
        pass

    @abstractmethod
    def add_case(self, case):
        pass


class Classifier(object):

    def __init__(self, name=None, sep='::', keys=None):
        self._name = name
        self._keys = keys or ['activity']
        self.sep = sep

    @property
    def name(self):
        return self._name

    def add_key(self, key):
        self._keys.append(key)

    @property
    def keys(self):
        return self._keys

    def get_event_name(self, event):
        name = []
        for k in self._keys:
            if hasattr(event, k):
                name.append(str(getattr(event, k)))
            elif k in event.attributes:
                name.append(event.attributes[k])
        if name:
            return self.sep.join(name)
        else:
            return str(event.id)





class ProcessLog(BaseLog):
    """
    A contaneir for :class:`Case<pymine.mining.process.eventlog.Case>`.They must belong to the same process.
    """
    def __init__(self, process, cases=None, filename=None, classifier=None):
        super(ProcessLog, self).__init__(filename)
        self._process = process
        self._cases = []
        if cases is not None:
            for case in cases:
                self.add_case(case)
        if classifier:
            self.classifier = classifier
        else:
            self.classifier = Classifier(process)
            self.classifier.add_key("activity")

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

    def __init__(self, cases=None, process_logs=(), filename=None):
        super(Log, self).__init__(filename)
        self._process_logs = OrderedDict()
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
    def cases(self):  # TODO: there is no order in the cases returned. Maybe this method is non very useful and can be removed
        cases = []
        for process_log in self._process_logs.values():
            cases.extend(process_log.cases)
        return cases

    def __iter__(self):
        return self.cases

    def __getitem__(self, item):
        process = self._process_logs.keys()[item]
        return self._process_logs[process]

    def get_process_log(self, process):
        return self._process_logs[process]

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


class AvroProcessLog(ProcessLog):
    def __init__(self, process, cases=None, filename=None):
        super(ProcessLog, self).__init__(filename)
        self._process = process

    def add_case(self, case):
        raise NotImplementedError

    @property
    def cases(self):
        from pymine.mining.process.eventlog.serializers.avro_serializer import deserialize
        if self.filename.startswith('hdfs://'):
            import pydoop.hdfs as hdfs
            fs = hdfs
        else:
            import os
            fs = os

        files = [fs.path.join(self.filename, f) for f in fs.listdir(self.filename)] \
            if fs.path.isdir(self.filename) else [self.filename]

        for fn in files:
            for c in deserialize(fn):
                yield c


class LogInfo(object):

    def __init__(self, log):
        self.log = log
        self._avg_lead_time =  None

    def compute_avg_lead_time(self):
        if self._avg_lead_time is None:

            avg = 0.0
            total_cases = 0
            for case in self.log.cases:
                total_cases += 1
                start_time = case.events[0].timestamp
                end_time = case.events[-1].timestamp
                if end_time and start_time:
                    avg += end_time - start_time

            self._avg_lead_time = avg/total_cases

        return self._avg_lead_time



