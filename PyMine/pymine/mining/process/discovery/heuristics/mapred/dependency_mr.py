import pymine.mining.process.discovery.heuristics.dependency as dp
from pymine.mining.mapred import MRLauncher
import os


class DependencyMiner(dp.DependencyMiner, MRLauncher):

    def _compute_precede_matrix(self):
        cwd = os.path.dirname(__file__)
        output_dir_prefix = "dm_output"

        self.run_mapred_job(self.log,
                            os.path.join(cwd, 'arc_info.avsc'),
                            os.path.join(cwd, 'arc_dep_mr.py'),
                            "arc_dep_mr",
                            output_dir_prefix)

        matrices = {
            'precede': self.precede_matrix,
            'two_step_loop': self.two_step_loop_freq,
            'long_distance': self.long_distance_freq
        }

        events = set()

        for avro_obj in self.avro_outputs:
            events.add(avro_obj['start_node'])
            events.add(avro_obj['end_node'])
            for n, m in matrices.items():
                m[avro_obj['start_node']][avro_obj['end_node']] += avro_obj[n]
            if avro_obj['is_start']:
                self.start_events.add(avro_obj['start_node'])
            if avro_obj['is_end']:
                self.end_events.add(avro_obj['end_node'])

        for e in events:
            freq = sum(self.precede_matrix[e].values())
            if not freq:
                cells = self.precede_matrix.get_column(e)
                freq = sum([c.value for c in cells])
            self.events_freq[e] = freq