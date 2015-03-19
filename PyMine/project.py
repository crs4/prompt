#from mining.process.discovery import promHeuristicMiner
from pymine.mining.process.discovery import hm

class Project():

    miner = hm.PromHeuristicMiner()

    def main(self, argv):
        self.miner.mine()


def main():
    proj = Project()
    proj.main(None)

if __name__ == '__main__':
    main()