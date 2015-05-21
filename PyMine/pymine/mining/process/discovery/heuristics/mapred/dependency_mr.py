import pymine.mining.process.discovery.heuristics.dependency as dp
from pymine.mining.process.discovery.heuristics.mapred import CLASSIFIER_FILENAME
from pymine.mining.mapred import MRLauncher, serialize_obj
import os


class DependencyMiner(dp.DependencyMiner, MRLauncher):
    def __init__(self, log, classifier=None, n_reducers=None, d_kwargs=None):
        dp.DependencyMiner.__init__(self, log, classifier)
        MRLauncher.__init__(self, n_reducers, d_kwargs)

    def _compute_precede_matrix(self):
        cwd = os.path.dirname(__file__)
        output_dir_prefix = "dm_output"
        classifier_filename = serialize_obj(self.classifier, 'classifier')
        self.d_kwargs[CLASSIFIER_FILENAME] = classifier_filename

        self.run_mapred_job(self.log,
                            os.path.join(cwd, 'arc_info.avsc'),
                            os.path.join(cwd, 'arc_dep_mr.py'),
                            "arc_dep_mr",
                            output_dir_prefix
                            )

        matrices = [
            self.precede_matrix,
            self.two_step_loop_freq,
            self.long_distance_freq
        ]

        events = set()

        for avro_obj in self.avro_outputs:
            events.add(avro_obj['start_node'])
            events.add(avro_obj['end_node'])
            for n, m in enumerate(matrices):
                m[avro_obj['start_node']][avro_obj['end_node']] += avro_obj['values'][n]

            is_start = avro_obj['values'][3]
            is_end = avro_obj['values'][4]
            if is_start:
                self.start_events.add(avro_obj['start_node'])
            if is_end:
                self.end_events.add(avro_obj['end_node'])

        for e in events:
            freq = sum(self.precede_matrix[e].values())
            if not freq:
                cells = self.precede_matrix.get_column(e)
                freq = sum([c.value for c in cells])
            self.events_freq[e] = freq