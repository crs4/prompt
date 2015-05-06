from pymine.mining.process.eventlog.factory import create_log_from_file
from pymine.mining.process.eventlog.serializers.avro_serializer import Serializer
import os


log_path = "/home/mauro/work/pymine/PyMine/dataset/pg_4.csv"
log = create_log_from_file(log_path)[0]
dest_path = "/home/mauro/avro_log_6mega"
if not os.path.exists(dest_path):
    os.mkdir(dest_path)

for idx, c in enumerate(log.cases):
    serializer = Serializer(os.path.join(dest_path, "%s.avro" % idx))
    print 'case', c
    for i in xrange(1000000):
        serializer.serialize(c)

    serializer.close()