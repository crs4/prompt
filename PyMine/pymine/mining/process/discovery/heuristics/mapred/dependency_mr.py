import pymine.mining.process.discovery.heuristics.dependency as dp
from pymine.mining.process.discovery.heuristics.mapred import CLASSIFIER_FILENAME
from pymine.mining.mapred import MRLauncher, serialize_obj
import os
import simplejson
import pydoop.hdfs as hdfs
import logging
logger = logging.getLogger('dependency_mr')


class DependencyMiner(dp.DependencyMiner, MRLauncher):
    def __init__(self, log, classifier=None, n_reducers=None, d_kwargs=None):
        dp.DependencyMiner.__init__(self, log, classifier)
        d_kwargs = d_kwargs or {}
        d_kwargs.update({'mapred.reduce.tasks': 1})
        MRLauncher.__init__(self, n_reducers, d_kwargs)

    def _compute_precede_matrix(self):
        cwd = os.path.dirname(__file__)
        output_dir_prefix = "dm_output"
        classifier_filename = serialize_obj(self.classifier, 'classifier')
        self.d_kwargs[CLASSIFIER_FILENAME] = classifier_filename

        self.run_mapred_job(self.log,
                            None,
                            os.path.join(cwd, 'arc_dep_mr.py'),
                            "arc_dep_mr",
                            output_dir_prefix,
                            )

        matrices = [
            self.precede_matrix,
            self.two_step_loop_freq,
            self.long_distance_freq
        ]

        events = set()

        print '-----self.output_dir', self.output_dir
        output_file = os.path.join(self.output_dir, 'part-r-00000')

        print 'OUTPUT FILE', output_file
        matrix = {}
        for output_file in hdfs.ls(self.output_dir):
            if os.path.basename(output_file).startswith('part'):
                with hdfs.open(output_file, 'r') as f:
                    for line in f:
                        json_obj = simplejson.loads(line)
                        matrix.update({tuple(json_obj[0]): json_obj[1]})

        logger.debug('matrix %s', matrix)
        for k, v in matrix.iteritems():
            e1, e2 = k
            events.add(e1)
            events.add(e2)
            for n, m in enumerate(matrices):
                m[e1][e2] += v[n]
            is_start = v[3]
            is_end = v[4]
            if is_start:
                self.start_events.add(e1)
            if is_end:
                self.end_events.add(e2)


        for e in events:
            freq = sum(self.precede_matrix[e].values())
            if not freq:
                cells = self.precede_matrix.get_column(e)
                freq = sum([c.value for c in cells])
            self.events_freq[e] = freq