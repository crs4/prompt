import sys
import os
import pydoop.hdfs as hdfs
import logging
logging.basicConfig(format="%(filename)s %(lineno)s %(levelname)s: %(message)s",
                    level=logging.DEBUG
                    )
logger = logging.getLogger('heuristic')
logger.setLevel(logging.DEBUG)

pymine_home = os.environ.get('PYMINE_HOME')
sys.path.append(pymine_home)
from pymine.mining.process.discovery.heuristics.dependency import DependencyGraph
from pymine.mining.process.discovery.heuristics.dependency import Matrix


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


def main(dir_path, dependency_thr):
    arcs = {}
    if hdfs.path.isdir(dir_path):
        for filename in hdfs.ls(dir_path):
            try:
                with hdfs.open(filename, "r") as fi:
                    while True:
                        try:
                            line = fi.readline()
                            index = line.index('[')
                            key = line[:index-1].strip()
                            values = line[index:].strip()
                            if key in arcs:
                                arcs[key] += values
                            else:
                                arcs[key] = values
                        except StopIteration:
                            break
            except Exception:
                continue
        for key in arcs:
            print("ARC: "+key)
            print("VALUE: "+str(arcs[key]))
            print("")
    else:
        raise Usage("Not a valid directory")
        sys.exit(0)

    '''
    dep_matrix = Matrix()
    _2_step_matrix = Matrix()

    # add the code to populate the matrices

    dm = DependencyMiner()
    dm._precede_matrix = dep_matrix
    dm._2_step_loop = _2_step_matrix
    dep_graph = dm.mine_dependency_graph(dependency_thr)
    '''

if __name__ == '__main__':
    import argparse
    logger.setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('file_path', type=str, help='the path of the hdfs directory containing the log')
    parser.add_argument('--aft', type=float, default=0.0, help="arc frequency threshold")
    parser.add_argument('--bft', type=float, default=0.0, help="binding frequency threshold")
    parser.add_argument('--dt', type=float, default=0.0, help="dependency threshold")

    args = parser.parse_args()
    main(args.file_path, args.dt)