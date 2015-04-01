import csv, sys
from pymine.mining.process.eventlog.log import Log, LogInfo, ProcessLog
from pymine.mining.process.eventlog import *
from pymine.mining.process.eventlog.exceptions import InvalidExtension
from mx.DateTime.Parser import DateTimeFromString
import logging
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

    def __init__(self, input_filename=None, add_start_activity=False, add_end_activity=False):
        super(CsvLogFactory, self).__init__()
        self.indexes = {}
        self._cases = {}
        self.activities = {}
        self._previous_case = None
        self.add_start_activity = add_start_activity
        self.add_end_activity = add_end_activity

        if input_filename:
            self.parse_csv_file(input_filename=input_filename)

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
        case_id = row[self.indexes['case_id']]
        timestamp = row[self.indexes['timestamp']]
        activity_id = row[self.indexes['activity']]
        resource = row[self.indexes['resource']]
        if case_id and activity_id:
            if case_id not in self._cases:

                if self.add_end_activity and self.cases:
                    self.cases[-1].add_event(FAKE_END)

                case = Case(_id=case_id, process=process)
                if self.add_start_activity:
                    case.add_event(FAKE_START)
                self._cases[case_id] = case

                self.cases.append(case)
                process.cases.append(case)
                #process.add_case(case)
            else:
                case = self._cases[case_id]

            if timestamp:
                timestamp = DateTimeFromString(timestamp)

            resources = resource.split('|') if resource else None

            attributes = []
            for attribute, index in self.indexes.items():
                if attribute not in ('case_id', 'timestamp', 'activity', 'resource'):
                    attribute_instance = Attribute(name=attribute, value=row[index])
                    attributes.append(attribute_instance)

            case.add_event(activity_id, timestamp, resources, attributes)

    def parse_csv_file(self, input_filename):
        with open(input_filename, 'rbU') as csvfile:
            # Check if first line has the parameters definition
            process = Process()
            first_line = csvfile.readline()
            dialect = csv.Sniffer().sniff(first_line)
            self.parse_indexes(first_line, dialect)
            # read the events data
            csvfile.seek(len(first_line))
            reader = csv.reader(csvfile, dialect)

            for row in reader:
                if row:
                    try:
                        self.parse_row(row, process)
                    except csv.Error as e:
                        logger.error(e)

            if self.add_end_activity and self.cases:
                self.cases[-1].add_event(FAKE_END)

    def create_log_from_file(self, input_filename):
        if input_filename:
            self.parse_csv_file(input_filename)
        return self.create_log()

    def create_loginfo_from_file(self, input_filename=None):
        if input_filename:
            self.parse_csv_file(input_filename)
        return self.create_loginfo()


class SimpleProcessLogFactory(LogFactory):

    def __init__(self, cases, process=None):
        self.cases = []
        self.process = process or Process()
        for case in cases:

            case_obj = Case(self.process)
            for event in case:
                activity = self.process.add_activity(event)
                activity_instance = case_obj.add_activity_instance(activity)
                activity_instance.add_event(activity_instance)
            self.cases.append(case_obj)

    def create_log(self):
        return ProcessLog(self.process, self.cases)


def create_log_from_csv(file_path, add_start_activity=False, add_end_activity=False):
    return CsvLogFactory(file_path, add_start_activity, add_end_activity).create_log()


def create_process_log_from_list(cases):
    process = Process()
    for case in cases:
        case_obj = process.add_case()
        for event in case:
            activity = process.add_activity(event)
            activity_instance = case_obj.add_activity_instance(activity)
            activity_instance.add_event(activity_instance)
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
            case.add_event(FAKE_START)

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
                case.add_event(activity_name,timestamp, resources, attributes)
            else:
                logger.warning('child %s should have at least concept:name and time:timestamp defined')

        if add_end_activity:
            case.add_event(FAKE_END)

    return Log(process.cases)


def create_log_from_file(file_path, add_start_activity=False, add_end_activity=False):
    ext = file_path.split('.')[-1].lower()
    valid_ext = ('csv', 'xes')
    if ext not in valid_ext:
        raise InvalidExtension('Unknown extension %s. Valid ones: %s' % (ext, valid_ext))
    if ext == 'csv':
        return create_log_from_csv(file_path, add_start_activity, add_end_activity)
    elif ext == 'xes':
        return create_log_from_xes(file_path,  add_start_activity, add_end_activity)


