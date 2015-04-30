import os
import tarfile
import subprocess
import uuid
from avro.datafile import DataFileReader
from avro.io import DatumReader
import pydoop.hdfs as hdfs
from pymine.mining.process.eventlog.serializers.avro_serializer import serialize_log_as_case_collection


def check_log_serilization(log):
    log_filename = log.filename
    serialized = False
    if log.filename is None or not log.filename.startswith('hdfs://'):
        # input_filename = 'hdfs:///user/%s/log.avro' % os.environ['USER']
        log_filename = 'hdfs:///user/%s/%s' % (os.environ['USER'], create_unique_filename(ext='avro'))
        serialize_log_as_case_collection(log, log_filename)
        serialized = True

    return log_filename, serialized


def create_unique_filename(prefix=None, ext=None):
    fn = str(uuid.uuid4())
    if prefix:
        fn = prefix + '_' + fn
    if ext:
        fn = fn + '.' + ext
    return fn


def create_code_archive(dest_path=None):
    cwd = os.path.dirname(__file__)
    dest_path = dest_path or "/tmp/pymine.tgz"
    tar = tarfile.open(dest_path, "w:gz")
    tar.add(os.path.join(cwd, '../../../../../mining'))
    tar.add(os.path.join(cwd, '../../../../../__init__.py'))
    tar.close()
    return dest_path


class MRMiner(object):
    def __init__(self):
        self.output_dir = None

    def run_mapred_job(self,
                       log,
                       output_schema_path,
                       mr_script_path,
                       mr_script_name,
                       output_dir_prefix,
                       n_reducer=1,  # FIXME
                       archive_path=None,
                       d_kwargs=None
                       ):

        input_filename, del_input_filename = check_log_serilization(log)
        d_kwargs = d_kwargs or {}

        with open(output_schema_path, 'r') as sf:
            schema_str = sf.read()

        self.output_dir = create_unique_filename(output_dir_prefix)

        archive_path = create_code_archive(archive_path)

        args = ["pydoop", "submit",]
        for k, v in d_kwargs.items():
            args += ["-D", "%s=%s" % (k, v)]

        args += [
            "-D", "pydoop.mapreduce.avro.value.output.schema=%s" % schema_str,
            "--upload-file-to-cache", mr_script_path,
            "--upload-archive-to-cache",
            archive_path,
            "--avro-input", "v",
            "--avro-output", "v",
            "--log-level", "DEBUG",
            "--num-reducers", str(n_reducer),
            "--mrv2",
            mr_script_name,
            input_filename,
            self.output_dir
        ]

        retcode = subprocess.call(args)
        # if retcode:
        #     raise Exception('mapred failed')  # FIXME on bruja succeeded jobs return retcode > 0 because of job status
        if del_input_filename:
            try:
                hdfs.rmr(input_filename)
            except IOError:
                pass

        # try:
        #     hdfs.rmr(self.output_dir)
        # except IOError:
        #     pass

    @property
    def avro_outputs(self):
        for filename in hdfs.ls(self.output_dir):
            if filename.split('.')[-1] == 'avro':
                with hdfs.open(filename, "r") as fi:
                    reader = DataFileReader(fi, DatumReader())
                    for e in reader:
                        yield e


