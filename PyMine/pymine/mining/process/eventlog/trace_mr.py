import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pp


class Mapper(api.Mapper):
    def map(self, context):
        raw_fields = context.value.split(',')
        case_id = context.job_conf.get_int('case_id')
        timestamp = context.job_conf.get_int('timestamp')
        activity = context.job_conf.get_int('activity')
        context.emit(raw_fields[case_id], [raw_fields[timestamp], raw_fields[activity]])


class Reducer(api.Reducer):
    def reduce(self, context):
        line = []
        for value in context.values:
            line.append(value)
        context.emit(context.key, line)


def __main__():
    CONF = {"case_id": 1,
            "timestamp": 0,
            "activity": 2}
    pp.run_task(pp.Factory(Mapper, Reducer))
