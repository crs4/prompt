import csv, sys
from pymine.mining.process.eventlog.log import Log, LogInfo
from pymine.mining.process.eventlog import *


class LogInfoFactory(object):

    def __init__(self, log=None):
        self.processes = log.processes if log else []

    def create_loginfo(self):
        loginfo = LogInfo()
        for process in self.processes:
            process_info = ProcessInfo()
            process_info.process = process
            process_info.activities_number = len(process.activities)
            process_info.cases_number = len(process.cases)
            events_number = 0
            for case in process.cases:
                events_number += len(case.events)
            process_info.events_number = events_number
            loginfo.processes_info[process.id] = process_info
        return loginfo

class LogFactory(object):

    def __init__(self, processes=[]):
        self.processes = processes

    def create_log(self):
        return Log(self.processes)

    def create_loginfo(self):
        log_info = LogInfo()
        for process in self.processes:
            process_info = ProcessInfo()
            process_info.process = process
            process_info.activities_number = len(process.activities)
            process_info.cases_number = len(process.cases)
            events_number = 0
            for case in process.cases:
                events_number += len(case.events)
            process_info.events_number = events_number
            log_info.processes_info[process.id] = process_info
        return log_info

class CsvLogFactory(LogFactory):

    def __init__(self, input_filename=None):
        super(CsvLogFactory, self).__init__()
        self.indexes = {}
        self.cases = {}
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
            print("An error occurred while parsing the field names: "+str(e))

    def parse_row(self, row, process):
        case_id = row[self.indexes['case_id']]
        timestamp = row[self.indexes['timestamp']]
        activity_id = row[self.indexes['activity']]
        resource = row[self.indexes['resource']]
        index = 0
        if case_id and activity_id:
            if case_id not in self.cases:
                case = Case(_id=case_id, process=process)
                self.cases[case_id] = case
                process.cases.append(case)
            else:
                case = self.cases[case_id]

            event = Event()
            case.events.append(event)

            if activity_id not in self.activities:
                activity = Activity(name=activity_id)
                self.activities[activity_id] = activity
                process.activities.append(activity)
            else:
                activity = self.activities[activity_id]

            activity_instance = ActivityInstance(activity=activity, events=[event])
            case.activity_instances.append(activity_instance)
            event.activity_instance = activity_instance

            if timestamp:
                tstamp = Attribute(value=timestamp, name="timestamp")
                tstamp.event = event
                event.timestamp = tstamp

            if resource:
                resources = resource.split('|')
                for res in resources:
                    rs = Attribute(value=res, name="resource")
                    rs.event = event
                    event.resources.append(rs)

            for attribute in self.indexes:
                if index != self.indexes['case_id'] and \
                                index != self.indexes['timestamp'] and \
                                index != self.indexes['activity'] and \
                                index != self.indexes['resource']:
                    attribute_instance = Attribute(name=attribute, value=row[self.indexes[attribute]], event=event)
                    event.attributes.append(attribute_instance)

    def parse_csv_file(self, input_filename):
        with open(input_filename, 'rbU') as csvfile:
            # Check if first line has the parameters definition
            process = Process()
            self.processes.append(process)
            first_line = csvfile.readline()
            dialect = csv.Sniffer().sniff(first_line)
            self.parse_indexes(first_line, dialect)
            # read the events data
            csvfile.seek(len(first_line))
            reader = csv.reader(csvfile, dialect)
            try:
                for row in reader:
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