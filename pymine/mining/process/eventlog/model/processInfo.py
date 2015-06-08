class ProcessInfo(object):

    def __init__(self):
        self.process_id = None
        self.activities_number = 0
        self.cases_number = 0
        self.average_case_size = 0
        self.events_number = 0
        self.activity_frequencies = []
        self.event_frequencies = []

    def __str__(self):
        doc = "process_id="+str(self.process_id)+"\n" \
        "activities_number="+str(self.activities_number)+"\n" \
        "cases_number="+str(self.cases_number)+"\n" \
        "average_case_size="+str(self.average_case_size)+"\n" \
        "events_number="+str(self.events_number)+"\n"
        return doc

