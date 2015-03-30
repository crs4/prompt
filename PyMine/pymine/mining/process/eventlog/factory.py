import csv, sys
from pymine.mining.process.eventlog.log import Log, LogInfo, ProcessLog
from pymine.mining.process.eventlog import *
import datetime
import logging
logger = logging.getLogger('factory')

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

    TIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

    def __init__(self, input_filename=None, time_format=TIME_FORMAT):
        super(CsvLogFactory, self).__init__()
        self.time_format=time_format
        self.indexes = {}
        self._cases = {}
        self.activities = {}
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

                case = Case(_id=case_id, process=process)
                self._cases[case_id] = case

                self.cases.append(case)
                process.cases.append(case)
                #process.add_case(case)
            else:
                case = self._cases[case_id]

            if activity_id not in self.activities:
                activity = process.add_activity(activity_id)
                self.activities[activity_id] = activity
                process.activities.append(activity)
            else:
                activity = self.activities[activity_id]

            activity_instance = case.add_activity_instance(activity)

            if timestamp:
                timestamp = datetime.datetime.strptime(timestamp, self.time_format)

            resources = resource.split('|') if resource else None

            attributes = []
            for attribute, index in self.indexes.items():
                if attribute not in ('case_id', 'timestamp', 'activity', 'resource'):
                    attribute_instance = Attribute(name=attribute, value=row[index])
                    attributes.append(attribute_instance)

            activity_instance.add_event(timestamp=timestamp, resources=resources, attributes=attributes)

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
            try:
                for row in reader:
                    if row:
                        self.parse_row(row, process)
            except csv.Error as e:
                sys.exit('file %s, line %d: %s' % (input_filename, reader.line_num, e))

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