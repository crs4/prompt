__author__ = 'paolo'

from jpype import *

from pymine.mining.process.discovery.heuristic import HeuristicMiner as HeuristicMiner


class PromHeuristicMiner(HeuristicMiner):

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

    def mine(self):
        try:
            startJVM(getDefaultJVMPath(), "-Djava.class.path=%s" % self.classpath)
            PluginPackage = JPackage('org').processmining.framework.plugin.impl
            XesXmlParser = JClass('org.deckfour.xes.in.XesXmlParser')
            #BSHClass = JClass('org.processmining.plugins.heuristicsnet.miner.dependency.miner.HeuristicsMiner')
            BSHClass = JClass('org.crs4.prom.dependency.ModifiedHeuristicsMiner')
            Is = java.io.FileInputStream("/Users/paolo/Desktop/Analytics/Paolo/test.xes")
            XLogParse = XesXmlParser()
            XLog = XLogParse.parse(Is)
            BSH = BSHClass(XLog)
            HNet = BSH.mine()
            shutdownJVM()
        except JavaException as exception:
            print exception.message()
            print exception.stacktrace()
        except Exception, e:
            print e