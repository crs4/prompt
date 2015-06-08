from pyspark import SparkContext
from pyspark import SparkContext, SparkConf

logFile = "t2ls_system_collectors.csv"  # Should be some file on your system
conf = SparkConf().setAppName("arc_dep_app").setMaster("yarn-client")
sc = SparkContext(conf=conf)
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)
