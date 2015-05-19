import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import pymine.mining.process.eventlog as el
from pymine.mining.process.eventlog.log import Log, AvroLog
from mx.DateTime.Parser import DateTimeFromString
import os
import collections


class InvalidSchema(Exception):
    pass


def is_hdfs(path):
    return path.startswith('hdfs://')


class Serializer(object):
    def __init__(self, path):
        self.path = path
        self.writer = None
        self.f_out = None
        self.schema_path = None

    def _create_writer(self, schema_path):
        if is_hdfs(self.path):
            import pydoop.hdfs as hdfs
            openfile = hdfs.open
        else:
            openfile = open

        with open(schema_path, 'r') as sf:
            self.schema = avro.schema.parse(sf.read())

        self.f_out = openfile(self.path, 'w')
        self.writer = DataFileWriter(self.f_out, DatumWriter(), self.schema)

    def serialize(self, objs):
        if not isinstance(objs, collections.Iterable):
            objs = [objs]

        if self.writer is None:

            schema_name = objs[0].__class__.__name__
            schema_path = os.path.join(os.path.dirname(__file__), 'schemas/%s.avsc' % schema_name)
            self._create_writer(schema_path)
        for obj in objs:
            self.writer.append(convert_obj_to_avro_dict(obj))

    def close(self):
        self.writer.close()
        self.f_out.close()




def convert_obj_to_avro_dict(obj):
    cls_name = obj.__class__
    if cls_name == el.Case:
        return {"id": str(obj.id), "events": [convert_obj_to_avro_dict(e) for e in obj.events]}
    elif cls_name == el.Event:
        return {"id": str(obj.id), "activity_name": obj.name, "timestamp": str(obj.timestamp)}


def serialize(objs, dest_path):
    serializer = Serializer(dest_path)
    serializer.serialize(objs)
    serializer.close()


def deserialize(path):
    if is_hdfs(path):
        import pydoop.hdfs as hdfs
        openfile = hdfs.open
    else:
        openfile = open

    reader = DataFileReader(openfile(path, "r"), DatumReader())
    return _AvroIterator(reader)


def convert_avro_dict_to_obj(avro_obj, schema):
    if schema == 'Log':
        process = el.Process()
        for case in avro_obj["cases"]:
            case_obj = convert_avro_dict_to_obj(case, "Case")
            process.add_case(case_obj)
        obj = Log(process, process.cases)

    elif schema == 'Case':
        obj = el.Case(_id=avro_obj["id"])
        for event in avro_obj["events"]:
            obj.add_event(convert_avro_dict_to_obj(event, "Event"))

    elif schema == 'Event':
        obj = el.Event(
            name=avro_obj["activity_name"],
            timestamp=DateTimeFromString(avro_obj['timestamp']),
            _id=str(avro_obj["id"]))
    else:
        raise InvalidSchema("invalid schema %s" % schema)

    return obj


def serialize_log_as_case_collection(log, dest_path):
    serialize(log.cases, dest_path)


def deserialize_log_from_case_collection(path):
    process = el.Process()
    return AvroLog(process, filename=path)

class _AvroIterator(object):

    def __init__(self, reader):
        self._reader = reader
        self.schema = reader.datum_reader.writers_schema

    def __iter__(self):
        for c in self._reader:
            yield convert_avro_dict_to_obj(c, self.schema.name)


