__author__ = 'paolo'

class LogInfo(object):

    def __init__(self):
        self.processes_info = {}

    def get_process_size(self):
        return len(self.processes)

    def get_process_info(self, process_id):
        if process_id in self.processes_info:
            return self.processes_info[process_id]
        else:
            return None

    def __str__(self):
        doc = ""
        for process in self.processes_info:
            doc += "{"+process+" : "+str(self.processes_info[process])+"}\n"
        return str(doc)
