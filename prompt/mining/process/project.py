#from mining.process.discovery import promHeuristicMiner
from prompt.mining.process.discovery import hm

class Project():

    miner = hm.PromHeuristicMiner()
    #miner = promHeuristicMiner.PromHeuristicMiner()

    def main(self, argv):
        self.miner.mine()


def main():
    proj = Project()
    proj.main(None)

if __name__ == '__main__':
    main()