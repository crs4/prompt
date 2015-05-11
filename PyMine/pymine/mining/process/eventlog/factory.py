import csv
import logging
from collections import defaultdict, OrderedDict
from mx.DateTime.Parser import DateTimeFromString

from pymine.mining.process.eventlog.log import Log, LogInfo
from pymine.mining.process.eventlog import *
from pymine.mining.process.eventlog.exceptions import InvalidExtension
import os
logger = logging.getLogger('factory')


FAKE_START = '_start'
FAKE_END = '_end'


class LifeCycle:
    START = 'start'
    END = 'end'


class LogInfoFactory(object):

    def __init__(self, log=None):
        self.processes = log.processes if log else []

    def create_loginfo(self):
        loginfo = LogInfo()
        for process in self.processes:
            process_info = ProcessInfo(process)
            loginfo.processes_info[process.id] = process_info
        return loginfo


class LogFactory(object):

    def __init__(self):
        self.cases = []

    def create_log(self):
        return Log(self.cases)


def add_event_to_case(case, event_name):
    activity = Activity(event_name)
    case.process.add_activity(activity)
    case.process.add_activity(activity)
    activity_instance = ActivityInstance()
    activity.add_activity_instance(activity_instance)
    event = Event(name=event_name)
    case.add_event(event)
    activity_instance.add_event(event)
    return event


class CsvLogFactory(LogFactory):

    def __init__(self, input_filename=None, add_start_activity=False, add_end_activity=False, create_process=False):
        super(CsvLogFactory, self).__init__()
        self.indexes = {}
        self._cases = OrderedDict()
        self.activities = {}
        self.activity_instances = {}
        self._previous_case = None
        self.add_start_activity = add_start_activity
        self.add_end_activity = add_end_activity
        self._pending_activity_instances = defaultdict(lambda: defaultdict(list))
        self.filename = input_filename
        self._process = None if not create_process else Process()
        if input_filename:
            self.parse_csv_file(input_filename, create_process)

    def create_log(self):
        log = Log(cases=self._cases.values(), filename=self.filename)
        log.process = self._process
        return log

    def parse_indexes(self, input_line, dialect):
        try:
            tokens = input_line.split(dialect.delimiter)
            counter = 0
            for token in tokens:
                self.indexes[token.rstrip()] = counter
                counter += 1
        except Exception, e:
            logger.error("An error occurred while parsing the field names: "+str(e))

    def parse_row(self, row, process):
        logger.debug('row %s', row)

        case_id = row[self.indexes['case_id']] if 'case_id' in self.indexes else None
        timestamp = row[self.indexes['timestamp']] if 'timestamp' in self.indexes else None
        activity_id = row[self.indexes['activity']] if 'activity' in self.indexes else None
        activity_instance_id = row[self.indexes['activity_instance']] if 'activity_instance' in self.indexes else None
        resource = row[self.indexes['resource']] if 'resource' in self.indexes else None
        lifecycle = row[self.indexes['lifecycle']].lower() if 'lifecycle' in self.indexes else None

        if case_id and activity_id:
            if case_id not in self._cases:
                case = Case()
                if self.add_start_activity:
                    case.add_event(Event(name=FAKE_START))

                if self.add_end_activity and self._cases.values():
                    # self.cases[-1].add_event(Event(FAKE_END))
                    self._cases.values()[-1].add_event(Event(name=FAKE_END))
                self._cases[case_id] = case

            else:
                case = self._cases.values()[-1]

            # self.cases.append(case)

            attributes = {}
            for attribute, index in self.indexes.items():
                # if attribute not in ('case_id', 'timestamp', 'activity', 'resource', 'activity_instance'):
                attributes[attribute] = row[index]

            if timestamp:
                timestamp = DateTimeFromString(timestamp)

            event = Event(name=activity_id,timestamp=timestamp, resources=resource, attributes=attributes)
            case.add_event(event)

            if process:
                _add_event_to_process(
                    self._process,
                    case,
                    self._pending_activity_instances,
                    event
                    )


            #
            # activity = process.get_activity_by_name(activity_id)
            # if activity is None:
            #     activity = Activity(activity_id)
            #     process.add_activity(activity)
            #
            # if activity_instance_id in self.activity_instances:
            #     activity_instance = self.activity_instances[activity_instance_id]
            # else:
            #     if not activity_instance_id:
            #         pending_activity_instances = self._pending_activity_instances[case_id]
            #         if lifecycle == LifeCycle.START or not pending_activity_instances[activity.name]:
            #             activity_instance = ActivityInstance()
            #             pending_activity_instances[activity.name].append(activity_instance)
            #         else:
            #             if pending_activity_instances[activity.name]:
            #                 activity_instance = pending_activity_instances[activity.name][0]
            #
            #             else:
            #                 activity_instance = ActivityInstance()
            #
            #         if lifecycle == LifeCycle.END:
            #             pending_activity_instances[activity.name].remove(activity_instance)
            #
            #     else:
            #         activity_instance = ActivityInstance(_id=activity_instance_id)
            #
            #     self.activity_instances[activity_instance.id] = activity_instance
            #     activity.add_activity_instance(activity_instance)
            #     case.add_activity_instance(activity_instance)
            #
            # if timestamp:
            #     timestamp = DateTimeFromString(timestamp)
            #
            # attributes = {}
            # for attribute, index in self.indexes.items():
            #     if attribute not in ('case_id', 'timestamp', 'activity', 'resource', 'activity_instance'):
            #         attributes[attribute] = row[index]
            #
            # event = Event(
            #     activity_instance=activity_instance,
            #     name=activity_instance.activity.name,
            #     timestamp=timestamp,
            #     resources=resource,
            #     attributes=attributes)
            #
            # case.add_event(event)
            # activity_instance.add_event(event)

    def parse_csv_file(self, input_filename, create_process):
        
        with open(input_filename, 'rbU') as csvfile:
            # Check if first line has the parameters definition
            first_line = csvfile.readline()
            dialect = csv.Sniffer().sniff(first_line)
            self.parse_indexes(first_line, dialect)
            # read the events data
            csvfile.seek(len(first_line))
            reader = csv.reader(csvfile, dialect)

            for row in reader:
                if row:
                    try:
                        self.parse_row(row, create_process)
                    except csv.Error as e:
                        logger.error(e)

        if self.add_end_activity and self._cases.values():
            # self.cases[-1].add_event(Event(FAKE_END))
           self._cases.values()[-1].add_event(Event(name=FAKE_END))

    def create_log_from_file(self, input_filename, create_process=False):
        if input_filename:
            self.parse_csv_file(input_filename, create_process)
        return self.create_log()


def create_log_from_csv(file_path, add_start_activity=False, add_end_activity=False, create_process=True):
    return CsvLogFactory(file_path, add_start_activity, add_end_activity, create_process).create_log()


def create_process_log_from_list(cases, create_process=True):
    case_objs = []
    for case in cases:
        case_obj = Case()
        for event_name in case:
            event = Event(name=event_name)
            case_obj.add_event(event)
        case_objs.append(case_obj)

    log = Log(case_objs)
    if create_process:
        process = build_process(log)
        log.process = process
    return log


def create_log_from_xes(file_path, add_start_activity=True, add_end_activity=True, create_process=True):
    import xml.etree.ElementTree as ET
    ns = {'xes': 'http://www.xes-standard.org/'}
    cases = []
    tree = ET.parse(file_path)
    xml_log = tree.getroot()
    for trace in xml_log.findall('xes:trace', ns):
        case = Case()
        cases.append(case)
        if add_start_activity:
            case.add_event(Event(name=FAKE_START))

        for event in trace.findall('xes:event', ns):
            timestamp = None
            attributes = []
            resources = []
            activity_name = None
            for child in list(event):
                if child.tag == '{%s}string' % ns['xes']:
                    if child.attrib['key'] == 'concept:name':
                        activity_name = child.attrib['value']
                elif child.tag == '{%s}date' % ns['xes']:
                    value = DateTimeFromString(child.attrib['value'])
                    if child.attrib['key'] == 'time:timestamp':
                        timestamp = value
                    else:
                        attributes.append(Attribute(name=child.attrib['key'], value=value))
                elif child.tag == '{%s}org:resource' % ns['xes']:
                    resources.append(child.attrib['value'])
                else:
                    attributes.append(Attribute(name=child.attrib['key'], value=child.attrib['value']))
            if activity_name:
                case.add_event(Event(activity_name, timestamp, resources, attributes))
            else:
                logger.warning('child %s should have at least concept:name and time:timestamp defined')

        if add_end_activity:
            case.add_event(Event(name=FAKE_END))

    log = Log(cases)
    if create_process:
        process = build_process(log)
        log.process = process
    return log


def create_log_from_file(file_path, add_start_activity=True, add_end_activity=True, create_process=True):

    ext = file_path.split('.')[-1].lower()
    valid_ext = ('csv', 'xes', 'avro')
    if file_path.startswith('hdfs://'):
        import pydoop.hdfs as fs
    else:
        fs = os
    is_dir = fs.path.isdir(file_path)  # FIXME should check if is a hdfs path
    if ext not in valid_ext and not is_dir:
        raise InvalidExtension('Unknown extension %s. Valid ones: %s' % (ext, valid_ext))
    if ext == 'csv':
        return create_log_from_csv(file_path, add_start_activity, add_end_activity, create_process)
    elif ext == 'xes':
        return create_log_from_xes(file_path,  add_start_activity, add_end_activity, create_process)
    elif ext == 'avro' or is_dir:
        from pymine.mining.process.eventlog.serializers.avro_serializer import deserialize_log_from_case_collection
        return deserialize_log_from_case_collection(file_path)


def build_process(log):
    process = Process()
    pending_activity_instances = defaultdict(lambda: defaultdict(list))
    for case in log.cases:
        for e in case.events:
            _add_event_to_process(
                process,
                case,
                pending_activity_instances,
                e
        )
    return process


def _add_event_to_process(
        process,
        case,
        pending_activity_instances,
        event
):
    activity = process.get_activity_by_name(event.name)
    if activity is None:
        activity = Activity(event.name)
        process.add_activity(activity)

    process.add_case(case)

    if event.activity_instance_id:
        activity_instance = case.get_activity_instance_by_id(event.activity_instance_id) or \
                            ActivityInstance(event.activity_instance_id)

    else:
        if event.lifecycle:
            lifecycle = event.lifecycle.lower()

            _pending_activity_instances = pending_activity_instances[case.id]
            if lifecycle == LifeCycle.START or not _pending_activity_instances[activity.name]:
                activity_instance = ActivityInstance()
                _pending_activity_instances[activity.name].append(activity_instance)
            else:
                if _pending_activity_instances[activity.name]:
                    activity_instance = _pending_activity_instances[activity.name][0]

                else:
                    activity_instance = ActivityInstance()

            if lifecycle == LifeCycle.END:
                _pending_activity_instances[activity.name].remove(activity_instance)

        else:
            activity_instance = ActivityInstance()

    activity.add_activity_instance(activity_instance)
    case.add_activity_instance(activity_instance)
    activity_instance.add_event(event)


