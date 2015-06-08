from prompt.mining.process.eventlog.exceptions import InvalidProcess
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

    @abstractmethod
    def __iter__(self):
        pass


class Classifier(object):
    DEFAULT_SEP = '::'

    def __init__(self, name=None, sep=DEFAULT_SEP, keys=None):
        self._name = name
        self._keys = keys or ['name']
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

            if k in event.attributes:
                name.append(event.attributes[k])
            elif hasattr(event, k):
                v = getattr(event, k)
                if v:
                    name.append(str(v))


            # elif k in event.attributes:
            #     v = event.attributes[k]
            #     if v:
            #         name.append(v)
        if name:
            return self.sep.join(name)
        else:
            return str(event.name)


class Log(BaseLog):
    def __init__(self, cases=None, filename=None):
        super(Log, self).__init__(filename)
        self._cases = cases or []

    @property
    def cases(self):
        return self._cases

    def add_case(self, case):
        self._cases.append(case)

    def __iter__(self):
        return self.cases


class AvroLog(BaseLog):
    def __init__(self, cases=None, filename=None):
        super(AvroLog, self).__init__(filename)

    def add_case(self, case):
        raise NotImplementedError

    @property
    def cases(self):
        from prompt.mining.process.eventlog.serializers.avro_serializer import deserialize
        if self.filename.startswith('hdfs://'):
            import pydoop.hdfs as hdfs
            fs = hdfs
            listdir = hdfs.ls
        else:
            import os
            fs = os
            listdir = os.listdir

        files = [fs.path.join(self.filename, f) for f in listdir(self.filename)] \
            if fs.path.isdir(self.filename) else [self.filename]

        for fn in files:
            for c in deserialize(fn):
                yield c

    def __iter__(self):
        return self.cases


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



