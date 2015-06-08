from pymine.mining.process.eventlog.factory import create_log_from_file
import pickle
import datetime as dt
from pymine.mining.process.eventlog.log import Classifier
from pymine.mining.process.eventlog import ProcessInfo


def main(log_path):
    log = create_log_from_file(log_path)
    #classifier = Classifier(keys=['activity', 'activity_type'])
    p_info = ProcessInfo(log.process)
    print "AVERAGE LEAD TIME: %s" % str(p_info.average_lead_time)
    print "LEAD TIME DATA: %s" % str(p_info.get_lead_time_data())
    print ""
    for activity in log.process.activities:
        print "ACTIVITY: %s" % activity.name
        print "AVERAGE DURATION TIME: %s" % str(p_info.get_average_activity_duration(activity.name))
        print "DURATION TIME DATA: %s" % str(p_info.get_activity_duration(activity.name))
        for instance in activity.activity_instances:
            print "ACTIVITY INSTANCE: %s" % str(instance)
        print ""


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create and show cnet mined from a csv log')
    parser.add_argument('log_path', type=str, help='the path of the log')

    args = parser.parse_args()
    main(args.log_path)
