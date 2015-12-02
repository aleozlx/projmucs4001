import os, sys, random, itertools, traceback
from pyspark import SparkContext

class Mixin(object):
    def __init__(self, sc):
        super(Mixin, self).__init__()
        self.sc=sc

    def inputData(self, serial, relation):
        return self.sc.textFile('hdfs://localhost:9000/user/hadoop/proj4001/inputs/%s/%s'%(serial, relation))

def getSparkContext(appname):
    sc=SparkContext("local", appname)
    sc.mixin=Mixin(sc)
    return sc

