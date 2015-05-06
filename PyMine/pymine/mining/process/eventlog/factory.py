import csv
import logging

from mx.DateTime.Parser import DateTimeFromString

from pymine.mining.process.eventlog.log import Log, LogInfo, ProcessLog
from pymine.mining.process.eventlog import *
from pymine.mining.process.eventlog.exceptions import InvalidExtension
import os
logger = logging.getLogger('factory')

FAKE_START = '_start'
FAKE_END = '_end'

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


class CsvLogFactory(LogFactory):

    def __init__(self, input_filename=None, add_start_activity=False, add_end_activity=False, classifier=None):
        super(CsvLogFactory, self).__init__()
        self.indexes = {}
        self._cases = {}
        self.activities = {}
        self.activity_instances = {}
        self._previous_case = None
        self.add_start_activity = add_start_activity
        self.add_end_activity = add_end_activity
        self._classifier = classifier
        self._process = Process()
        if input_filename:
            self.parse_csv_file(input_filename=input_filename)

    def create_log(self):
        return ProcessLog(self._process, cases=self.cases, classifier=self._classifier)

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
        label = None
        if self._classifier:
            if self._classifier.keys() in self.indexes:
                label = self._classifier.keys()
        if case_id and activity_id:
            if case_id not in self._cases:

                if self.add_end_activity and self.cases:
                    self.cases[-1].add_event(Event(FAKE_END))

                case = Case(_id=case_id)
                if self.add_start_activity:
                    case.add_event(Event(FAKE_START))
                self._cases[case_id] = case

                self.cases.append(case)
                process.add_case(case)
            else:
                case = self._cases[case_id]

            activity = process.get_activity_by_name(activity_id)
            if activity is None:
                activity = Activity(activity_id)
                process.add_activity(activity)

            if activity_instance_id in self.activity_instances:
                activity_instance = self.activity_instances[activity_instance_id]
            else:
                if not activity_instance_id:
                    activity_instance = ActivityInstance()
                else:
                    activity_instance = ActivityInstance(_id=activity_instance_id)

                self.activity_instances[activity_instance.id] = activity_instance

            activity.add_actity_instance(activity_instance)

            case.add_activity_instance(activity)
            case.add_activity_instance(activity_instance)

            if timestamp:
                timestamp = DateTimeFromString(timestamp)

            resources = resource.split('|') if resource else None

            attributes = []
            for attribute, index in self.indexes.items():
                if attribute not in ('case_id', 'timestamp', 'activity', 'resource'):
                    attribute_instance = Attribute(name=attribute, value=row[index])
                    attributes.append(attribute_instance)

            event = Event(timestamp=timestamp, resources=resources, attributes=attributes,
                                 activity_instance=activity_instance)
            case.add_event(event)
            activity_instance.add_event(event)

    def parse_csv_file(self, input_filename):
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
                        self.parse_row(row, self._process)
                    except csv.Error as e:
                        logger.error(e)

            if self.add_end_activity and self.cases:
                self.cases[-1].add_event(Event(FAKE_END))

    def create_log_from_file(self, input_filename):
        if input_filename:
            self.parse_csv_file(input_filename)
        return self.create_log()

    def create_loginfo_from_file(self, input_filename=None):
        if input_filename:
            self.parse_csv_file(input_filename)
        return self.create_loginfo()


def create_log_from_csv(file_path, add_start_activity=False, add_end_activity=False):
    return CsvLogFactory(file_path, add_start_activity, add_end_activity).create_log()


def create_process_log_from_list(cases):
    process = Process()
    for case in cases:
        case_obj = Case()
        process.add_case(case_obj)
        for event_name in case:
            case_obj.add_event(Event(name=event_name))
    return ProcessLog(process, process.cases)  # FIXME ProcessLog == Process...


def create_log_from_xes(file_path, add_start_activity=True, add_end_activity=True):
    import xml.etree.ElementTree as ET
    ns = {'xes': 'http://www.xes-standard.org/'}

    tree = ET.parse(file_path)
    xml_log = tree.getroot()
    process = Process()
    for trace in xml_log.findall('xes:trace', ns):
        case = process.add_case()
        if add_start_activity:
            case.add_event(Event(FAKE_START))

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
            case.add_event(Event(FAKE_END))

    return Log(process.cases)


def create_log_from_file(file_path, add_start_activity=False, add_end_activity=False):

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
        return create_log_from_csv(file_path, add_start_activity, add_end_activity)
    elif ext == 'xes':
        return create_log_from_xes(file_path,  add_start_activity, add_end_activity)
    elif ext == 'avro' or is_dir:
        from pymine.mining.process.eventlog.serializers.avro_serializer import deserialize_log_from_case_collection
        return deserialize_log_from_case_collection(file_path)



