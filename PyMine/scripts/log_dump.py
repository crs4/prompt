from pymine.mining.process.eventlog.factory import create_log_from_file
from pymine.mining.process.eventlog.serializers.avro_serializer import Serializer
import os


log_path = "/home/mauro/work/pymine/PyMine/dataset/pg_4.csv"
log = create_log_from_file(log_path)[0]
dest_path = "/home/mauro/avro_log"

for idx, c in enumerate(log.cases):
    serializer = Serializer(os.path.join(dest_path, "%s.avro" % idx))
    print 'case', c
    for i in xrange(100000):
        serializer.serialize(c)

    serializer.close()

from pymine.mining.process.eventlog.serializers.avro_serializer import deserialize_log_from_case_collection
l = deserialize_log_from_case_collection(dest_path)
i = 0
for c in l.cases:
    i += 1

print i
