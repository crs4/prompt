import os
import tarfile
import subprocess
import uuid
from avro.datafile import DataFileReader
from avro.io import DatumReader
from prompt.mining.process.eventlog.serializers.avro_serializer import serialize_log_as_case_collection
from prompt.mining.process.eventlog.serializers.avro_serializer import convert_avro_dict_to_obj
import pydoop.hdfs as hdfs
import pydoop.avrolib as avrolib
import pickle


def check_log_serilization(log):
    log_filename = log.filename
    moved_on_hdfs = False
    if log.filename is None or not log.filename.startswith('hdfs://'):
        log_filename = 'hdfs:///user/%s/%s' % (os.environ['USER'],  create_unique_filename(ext='avro'))
        # if not hdfs.path.exists(log_filename):
        if not log.filename or log.filename.split('.')[-1] != 'avro':
            serialize_log_as_case_collection(log, log_filename)
        else:
            hdfs.put(log.filename, log_filename)
        moved_on_hdfs = True
    return log_filename, moved_on_hdfs


def create_unique_filename(prefix=None, ext=None):
    fn = str(uuid.uuid4())
    if prefix:
        fn = prefix + '_' + fn
    if ext:
        fn = fn + '.' + ext
    return fn


def create_code_archive(dest_path=None):
    cwd = os.path.dirname(__file__)
    dest_path = dest_path or "/tmp/prompt.tgz"
    tar = tarfile.open(dest_path, "w:gz")
    tar.add(os.path.join(cwd, '../mining'))
    tar.add(os.path.join(cwd, '../__init__.py'))
    tar.close()
    return dest_path


def serialize_obj(obj, prefix):
    str_obj = pickle.dumps(obj)
    cnet_filename = create_unique_filename(prefix=prefix, ext="pkl")
    with hdfs.open(cnet_filename, 'w') as f:
        f.write(str_obj)
    return cnet_filename


def deserialize_obj(path):
    with hdfs.open(path, 'r') as f:
        obj = pickle.loads(f.read())
    return obj


class MRLauncher(object):
    def __init__(self, n_reducers=None, d_kwargs=None):
        self.output_dir = None
        self.n_reducers = n_reducers
        self.d_kwargs = d_kwargs or {}

    def run_mapred_job(self,
                       log,
                       output_schema_path,
                       mr_script_path,
                       mr_script_name,
                       output_dir_prefix,
                       archive_path=None,
                       log_level=None
                       ):

        input_filename, del_input_filename = check_log_serilization(log)

        if output_schema_path:
            with open(output_schema_path, 'r') as sf:
                schema_str = sf.read()
        else:
            schema_str = None

        self.output_dir = create_unique_filename(output_dir_prefix)

        args = ["pydoop", "submit"]
        if self.n_reducers is not None:
            args += ['--num-reducers', str(self.n_reducers)]
        for k, v in self.d_kwargs.items():
            args += ["-D", "%s=%s" % (k, v)]

        if schema_str:
            args += [
                "-D", "pydoop.mapreduce.avro.value.output.schema=%s" % schema_str,
                "--avro-output", "v"
            ]

        if archive_path:
            archive_path = create_code_archive(archive_path)
            args += ["--upload-archive-to-cache", archive_path]

        if log_level:
            args += ["--log-level", log_level]

        args += [
            "--upload-file-to-cache", mr_script_path,
            "--avro-input", "v",
            "--mrv2",
            mr_script_name,
            input_filename,
            self.output_dir
        ]
        ret_code = subprocess.call(args)
        if ret_code:
            raise RuntimeError('mapred failed')
        if del_input_filename:
            try:
                hdfs.rmr(input_filename)
            except IOError:
                pass

    @property
    def avro_outputs(self):
        for filename in hdfs.ls(self.output_dir):
            if filename.split('.')[-1] == 'avro':
                with hdfs.open(filename, "r") as fi:
                    reader = DataFileReader(fi, DatumReader())
                    for e in reader:
                        yield e


class CaseContext(avrolib.AvroContext):

    def deserializing(self, meth, datum_reader):
        def deserialize_and_wrap(*args, **kwargs):
            deserialize = super(CaseContext, self).deserializing(
                meth, datum_reader
            )
            return convert_avro_dict_to_obj(deserialize(*args, **kwargs), 'Case')
        return deserialize_and_wrap
