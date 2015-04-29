import pydoop.hdfs as hdfs
from avro.datafile import DataFileReader
from avro.io import DatumReader
from pymine.mining.process.discovery.heuristics import Matrix
import pymine.mining.process.discovery.heuristics.dependency as dp
from pymine.mining.process.eventlog.serializers.avro_serializer import serialize_log_as_case_collection
import os
import subprocess
import tarfile


class DependencyMiner(dp.DependencyMiner):

    def _compute_precede_matrix(self):
        cwd = os.path.dirname(__file__)
        output_dir = "dm_output"
        if self.log.filename is None or not self.log.filename.startswith('hdfs://'):
            input_filename = 'hdfs:///user/%s/log.avro' % os.environ['USER']
            try:
                hdfs.rmr(input_filename)
            except IOError:
                pass
            serialize_log_as_case_collection(self.log, input_filename)

        else:
            input_filename = self.log.filename

        with open(os.path.join(cwd, 'arc_info.avsc'), 'r') as sf:
            schema = sf.read()

        try:
            hdfs.rmr(output_dir)
        except IOError:
            pass

        tar = tarfile.open("/tmp/pymine.tgz", "w:gz")
        tar.add(os.path.join(cwd, '../../../../../mining'))
        tar.add(os.path.join(cwd, '../../../../../__init__.py'))
        tar.close()

        args = ["pydoop", "submit",
                "-D", "pydoop.mapreduce.avro.value.output.schema=%s" % schema,
                "--upload-file-to-cache", os.path.join(cwd, 'arc_dep_mr.py'),
                "--upload-archive-to-cache",
                "/tmp/pymine.tgz",
                "--avro-input", "v",
                "--avro-output", "v",
                "--log-level", "DEBUG",
                "--num-reducers", "1",  # FIXME
                "--mrv2",
                "arc_dep_mr",
                input_filename,
                output_dir]

        retcode = subprocess.call(args)
        # if retcode:
        #     raise Exception('mapred failed')

        matrices = {
            'precede': self.precede_matrix,
            'two_step_loop': self.two_step_loop_freq,
            'long_distance': self.long_distance_freq
        }

        events = set()
        for filename in hdfs.ls(output_dir):
            if filename.split('.')[-1] == 'avro':
                with hdfs.open(filename, "r") as fi:
                    reader = DataFileReader(fi, DatumReader())
                    for e in reader:
                        events.add(e['start_node'])
                        events.add(e['end_node'])
                        for n, m in matrices.items():
                            m[e['start_node']][e['end_node']] += e[n]
                        if e['is_start']:
                            self.start_events.add(e['start_node'])
                        if e['is_end']:
                            self.end_events.add(e['end_node'])

        for e in events:
            freq = sum(self.precede_matrix[e].values())
            if not freq:
                cells = self.precede_matrix.get_column(e)
                freq = sum([c.value for c in cells])
            self.events_freq[e] = freq