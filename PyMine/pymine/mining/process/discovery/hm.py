'''
classpath = "/Users/paolo/ProM/ProM-nightly-20150109-1.6/lib/bsh-2.0b4.jar:" \
                "/Users/paolo/ProM/ProM-nightly-20150109-1.6/lib/ProM-Contexts.jar:" \
                "/Users/paolo/ProM/ProM-nightly-20150109-1.6/lib/ProM-Models.jar:" \
                "/Users/paolo/ProM/ProM-nightly-20150109-1.6/lib/ProM-Plugins.jar:" \
                "/Users/paolo/ProM/ProM-nightly-20150109-1.6/lib/ProM-Framework.jar:" \
                "/Users/paolo/ProM/ProM-nightly-20150109-1.6/lib/guava-16.0.1.jar:" \
                "/Users/paolo/ProM/OpenXES-2.0/lib/OpenXES-XStream.jar:" \
                "/Users/paolo/ProM/OpenXES-2.0/lib/OpenXES.jar:" \
                "/Users/paolo/Downloads/HeuristicsMiner.jar:" \
                "/Users/paolo/PycharmProjects/ProcessMining/lib/mdh.jar"
'''
classpath = "/Users/paolo/PycharmProjects/PyMine/lib/bsh-2.0b4.jar:" \
                "/Users/paolo/PycharmProjects/PyMine/lib/ProM-Contexts.jar:" \
                "/Users/paolo/PycharmProjects/PyMine/lib/ProM-Models.jar:" \
                "/Users/paolo/PycharmProjects/PyMine/lib/ProM-Plugins.jar:" \
                "/Users/paolo/PycharmProjects/PyMine/lib/ProM-Framework.jar:" \
                "/Users/paolo/PycharmProjects/PyMine/lib/guava-16.0.1.jar:" \
                "/Users/paolo/PycharmProjects/PyMine/lib/OpenXES-XStream.jar:" \
                "/Users/paolo/PycharmProjects/PyMine/lib/OpenXES.jar:" \
                "/Users/paolo/PycharmProjects/PyMine/lib/HeuristicsMiner.jar:" \
                "/Users/paolo/PycharmProjects/PyMine/lib/mdh.jar"

import jnius_config
jnius_config.add_options('-Xrs', '-Xmx4096')
jnius_config.set_classpath('.', classpath)
from jnius import autoclass

from pymine.mining.process.discovery.heuristic import HeuristicMiner as HeuristicMiner

class PromHeuristicMiner(HeuristicMiner):

    def mine(self):
        XesXmlParser = autoclass('org.deckfour.xes.in.XesXmlParser')
        BSHClass = autoclass('org.crs4.prom.dependency.ModifiedHeuristicsMiner')
        FI = autoclass('java.io.FileInputStream')

        Is = FI("/Users/paolo/Desktop/Analytics/Paolo/test.xes")
        XLogParse = XesXmlParser()
        XLogList = XLogParse.parse(Is)
        XLog = XLogList.get(0)

        BSH = BSHClass(XLog)
        HNet = BSH.mine()

