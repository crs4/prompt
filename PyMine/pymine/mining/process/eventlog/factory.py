import csv
import logging
from collections import defaultdict, OrderedDict
from mx.DateTime.Parser import DateTimeFromString
from pymine.mining.process.eventlog.serializers.avro_serializer import serialize_log_as_case_collection
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
        log = Log(cases=self.cases, filename=self.filename)
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

    def parse_row(self, row, process, previous_case=None):
        logger.debug('row %s', row)

        case_id = row.get('case_id')
        timestamp = row.get('timestamp')
        activity_id = row.get('activity')
        activity_instance_id = row.get('activity_instance')
        resource = row.get('resource')
        lifecycle = row.get('lifecycle')

        if case_id and activity_id:
            if previous_case is not None and case_id == previous_case.id:
                case = previous_case
            else:
                case = Case(_id=case_id)
                self.cases.append(case)
                if self.add_start_activity:
                    case.add_event(Event(name=FAKE_START))
                if previous_case is not None and self.add_end_activity:
                    previous_case.add_event(Event(name=FAKE_END))

            # if len(case.events) > 0:
            #     previous_event = case.events[-1]
            #     if previous_event.attributes == row:
            #         print 'skipping duplicate event', row
            #         return

            if timestamp:
                timestamp = DateTimeFromString(timestamp)

            event = Event(name=activity_id,timestamp=timestamp, resources=resource, attributes=row)
            case.add_event(event)

            if process:
                _add_event_to_process(
                    self._process,
                    case,
                    self._pending_activity_instances,
                    event
                    )
            return case

    def parse_csv_file(self, input_filename, create_process):
        
        with open(input_filename, 'rbU') as csvfile:
            # Check if first line has the parameters definition
            first_line = csvfile.readline()
            dialect = csv.Sniffer().sniff(first_line)
            csvfile.seek(0)
            # self.parse_indexes(first_line, dialect)
            # # read the events data
            # csvfile.seek(len(first_line))
            # reader = csv.reader(csvfile, dialect)
            reader = csv.DictReader(csvfile, dialect=dialect)
            previous_case = None
            for row in reader:
                if row:
                    try:
                        previous_case = self.parse_row(row, create_process, previous_case)
                    except csv.Error as e:
                        logger.error(e)

        if self.add_end_activity and self.cases:
            # self.cases[-1].add_event(Event(FAKE_END))
            self.cases[-1].add_event(Event(name=FAKE_END))

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
        print trace
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

    log = Log(cases, file_path)
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


def export_log(log, dest_filename):
    ext = dest_filename.split('.')[-1]
    if ext != 'avro':
        raise InvalidExtension('exporting in file .%s is not supported yet' % ext)
    serialize_log_as_case_collection(log, dest_filename)


def convert_log(source_filename, dest_filename):
    log = create_log_from_file(source_filename, False, False, False)
    export_log(log, dest_filename)
