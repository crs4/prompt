import uuid
import logging
import mx.DateTime as dt

class IdObject(object):
    def __init__(self, _id=None):
        self.id = _id or uuid.uuid4()


class Process(IdObject):

    def __init__(self, _id=None):
        """

        :return:
        """
        super(Process, self).__init__(_id)
        self._activities = {}
        self._cases = {}

    @property
    def cases(self):
        return self._cases.values()

    def add_activity(self, activity):
        if activity.name in self._activities:
            return self._activities[activity.name]

        activity.process = self
        self._activities[activity.name] = activity
        return activity

    def get_activity_by_name(self, name):
        return self._activities.get(name)

    @property
    def activities(self):
        return self._activities.values()

    def add_case(self, case):
        case.process = self
        self._cases[case.id] = case
        return case

    def get_case_by_id(self, case_id):
        return self._cases.get(case_id)

class Activity(IdObject):

    def __init__(self, name, _id=None):
        """

        :return:
        """
        super(Activity, self).__init__(_id)
        self.name = name
        self.activity_instances = []
        self.process = None

    def __str__(self):
        return self.name

    def __hash__(self):
        return hash(self.name)

    def add_activity_instance(self, activity_instance):
        if activity_instance not in self.activity_instances:
            self.activity_instances.append(activity_instance)
            activity_instance.activity = self
        return activity_instance


class Case(IdObject):
    """
    It represents a sequence of :class:`Event<.Event>`
    """

    def __init__(self, _id=None):
        super(Case, self).__init__(_id)
        self.process = None
        self._activity_instances = {}
        self.events = []

    def get_activity_instance_by_id(self, _id):
        return self._activity_instances.get(_id)

    def add_activity_instance(self, activity_instance):
        self._activity_instances[activity_instance.id] = activity_instance
        activity_instance.case = self
        return activity_instance

    @property
    def activity_instances(self):
        return self._activity_instances.values()

    def add_event(self, event):
        event.case = self
        self.events.append(event)
        return event

    def __str__(self):
        return str([e.name for e in self.events])

    def __repr__(self):
        return str(self)

    # def __eq__(self, other):
    #     if type(self) == type(other):
    #         logging.debug('self %s != other %s', self, other)
    #         logging.debug("self.events %s", [e.activity_instance.activity.name for e in self.events])
    #         logging.debug("other.events %s", [e.activity_instance.activity.name for e in other.events])
    #
    #         return set(self.activity_instances) == set(other.activity_instances) \
    #             and set(self.events) == set(other.events)
    #
    #
    #     return False


class ActivityInstance(IdObject):

    def __init__(self, _id=None):
        super(ActivityInstance, self).__init__(_id)
        self.case = None
        self.activity = None
        self.events = []

    # def __eq__(self, other):
    #     if type(self) == type(other):
    #         try:
    #             for counter in xrange(len(self.events)):
    #                 assert self.events[counter] == other.events[counter]
    #         except AssertionError, e:
    #             logging.debug('self %s ! = other %s', self, other)
    #             return False
    #         return True
    #     else:
    #         return False

    def add_event(self, event):
        self.events.append(event)
        event.activity_instance = self
        return event

    def __str__(self):
        return self.id


class Event(IdObject):

    def __init__(self, activity_instance=None, name=None, timestamp=None, resources=None, attributes=None, _id=None, case=None):
        """

        :param name:
        :param timestamp: a datetime
        :param activity_instance:
        :param resources: a list of objects
        :param attributes: a list of :class:`Attribute<pymine.mining.process.eventlog.Attribute>`
        :param _id: a unique id
        :return:
        """
        super(Event, self).__init__(_id)
        self.name = name
        self.case = case
        self.attributes = attributes or {}
        self.timestamp = timestamp
        self.resources = resources
        self.activity_instance = activity_instance

    @property
    def activity(self):
        return self.activity_instance.activity if self.activity_instance else None

    def add_attribute(self, name, value):
        self.attributes[name] = value

    def __str__(self):
        return "timestamp %s, resources %s" % (self.timestamp, self.resources)

    def __getattr__(self, item):
        return self.attributes.get(item)


class Attribute(IdObject):

    def __init__(self, _id=None, name=None, value=None, event=None):
        super(Attribute, self).__init__(_id)
        self.name = name
        self.value = value
        self.event = event

    def __eq__(self, other):
        if type(self) == type(other):
            try:
                assert self.name == other.name
                assert self.value == other.value
            except AssertionError, e:
                logging.debug('self %s != other %s', self, other)
                return False
            return True
        else:
            return False

    def __str__(self):
        return "name: %s, value %s" % (self.name, self.value)


class ProcessInfo(object):

    def __init__(self, process):

        self.process = process
        self.activities_number = len(process.activities)
        self.cases_number = len(process.cases)
        events_number = 0
        for case in process.cases:
            events_number += len(case.events)
        self.events_number = events_number
        self.average_case_size = float(events_number/len(process.cases))
        self._average_lead_time = 0

        # self.activity_frequencies = activity_frequencies or []
        # self.event_frequencies = event_frequencies or []

    @property
    def average_lead_time(self):
        lead_time = 0
        for case in self.process.cases:
            lead_time += float(case.activity_instances[-1].events[-1].timestamp-case.activity_instances[0].events[0].timestamp)
        self._average_lead_time = float(lead_time/len(self.process.cases))
        return self._average_lead_time

    def get_lead_time_data(self):
        lead_time_data = []
        for case in self.process.cases:
            lead_time_data.append(float(case.activity_instances[-1].events[-1].timestamp-case.activity_instances[0].events[0].timestamp))
        return lead_time_data

    def get_activity_duration(self, activity_name):
        activity = self.process.get_activity_by_name(activity_name)
        if activity:
            activity_data = []
            for instance in activity.activity_instances:
                if len(instance.events) > 1:
                    activity_data.append(float(instance.events[-1].timestamp-instance.events[0].timestamp))
                else:
                    print "NONE"
            return activity_data
        else:
            None

    def get_average_activity_duration(self, activity_name):
        duration_data = self.get_activity_duration(activity_name)
        if duration_data:
            return sum(duration_data)/len(duration_data)
        else:
            return 0