__author__ = 'paolo'

import csv, sys
from log import Log
from loginfo import LogInfo
from model.case import Case
from model.activity import Activity
from model.attribute import Attribute
from model.attribute import Timestamp
from model.attribute import Resource
from model.activityinstance import ActivityInstance
from model.event import Event
from model.process import Process
from model.processInfo import ProcessInfo

class LogInfoFactory(object):

    def __init__(self, log=None):
        self.processes = {}
        self.cases = {}
        self.events = {}
        self.activities = {}
        self.activity_instances = {}
        self.attributes = {}
        if log:
            self.processes = log.processes
            self.cases = log.cases
            self.events = log.events
            self.activities = log.activities
            self.activity_instances = log.activity_instances
            self.attributes = log.attributes

    def create_loginfo(self):
        loginfo = LogInfo()
        for process_id in self.processes:
            process_info = ProcessInfo()
            process = self.processes[process_id]
            process_info.process_id = process_id
            process_info.activities_number = len(process.activities)
            process_info.cases_number = len(process.cases)
            events_number = 0
            for case in process.cases:
                events_number += len(self.cases[case].events)
            process_info.events_number = events_number
            loginfo.processes_info[process_id] = process_info
        return loginfo

class LogFactory(object):

    def __init__(self, processes=None, cases=None, events=None, activities=None, activity_instances=None, attributes=None):
        self.processes = {}
        self.cases = {}
        self.events = {}
        self.activities = {}
        self.activity_instances = {}
        self.attributes = {}
        if processes:
            self.processes = processes
        if cases:
            self.cases = cases
        if events:
            self.events = events
        if activities:
            self.activities
        if activity_instances:
            self.activity_instances = activity_instances
        if attributes:
            self.attributes = attributes

    def create_log(self):
        log = Log()
        log.cases = self.cases
        log.processes = self.processes
        log.activities = self.activities
        log.activity_instances = self.activity_instances
        log.attributes = self.attributes
        log.events = self.events
        return log

    def create_loginfo(self):
        loginfo = LogInfo()
        for process_id in self.processes:
            process_info = ProcessInfo()
            process = self.processes[process_id]
            process_info.process_id = process_id
            process_info.activities_number = len(process.activities)
            process_info.cases_number = len(process.cases)
            events_number = 0
            for case in process.cases:
                events_number += len(self.cases[case].events)
            process_info.events_number = events_number
            loginfo.processes_info[process_id] = process_info
        return loginfo

class CsvLogFactory(LogFactory):

    def __init__(self, input_filename=None):
        super(CsvLogFactory, self).__init__()
        self.indexes = {}
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

    def parse_row(self, row, process_id):
        case_id = row[self.indexes['case_id']]
        timestamp = row[self.indexes['timestamp']]
        activity = row[self.indexes['activity']]
        resource = row[self.indexes['resource']]
        index = 0
        if case_id and activity:
            if case_id not in self.cases:
                case = Case(id=case_id, process_id=process_id)
                self.cases[case_id] = case
                self.processes[process_id].cases.append(case_id)

            event = Event(case_id=self.cases[case_id].id)
            self.cases[case_id].events.append(event.id)

            if activity not in self.activities:
                activity_definition = Activity(name=activity)
                self.activities[activity_definition.id] = activity_definition
                self.processes[process_id].activities.append(activity)

            activity_instance = ActivityInstance(case_id=case_id, activity_id=self.activities[activity].id, events=[event.id])
            self.activity_instances[activity_instance.id] = activity_instance
            self.cases[case_id].activity_instances.append(activity_instance.id)
            event.activity_instance_id = activity_instance.id

            if timestamp:
                timestamp = Timestamp(attribute_value=timestamp, event_id=event.id)
                event.attributes.append(timestamp.id)
                self.attributes[timestamp.id] = timestamp

            if resource:
                resource = Resource(attribute_value=resource, event_id=event.id)
                event.attributes.append(resource.id)
                self.attributes[resource.id] = resource

            for attribute in self.indexes:
                if index != self.indexes['case_id'] and index != self.indexes['timestamp'] and index != self.indexes['activity'] and index != self.indexes['resource']:
                    attribute_instance = Attribute(attribute_name=attribute, attribute_value=row[self.indexes[attribute]], event_id=event.id)
                    event.attributes.append(attribute)
                    self.attributes[attribute_instance.id] = attribute_instance

            self.events[event.id] = event
        else:
            return

    def parse_csv_file(self, input_filename):
        with open(input_filename, 'rbU') as csvfile:
            # Check if first line has the parameters definition
            process = Process()
            self.processes[process.id] = process
            first_line = csvfile.readline()
            dialect = csv.Sniffer().sniff(first_line)
            self.parse_indexes(first_line, dialect)
            # read the events data
            csvfile.seek(len(first_line))
            reader = csv.reader(csvfile, dialect)
            try:
                for row in reader:
                    self.parse_row(row, process.id)
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