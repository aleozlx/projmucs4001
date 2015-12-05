import os, subprocess, sys, random, itertools, traceback
from pyspark import SparkContext

class Mixin(object):
    def __init__(self, sc):
        super(Mixin, self).__init__()
        self.sc=sc

    def inputData(self, serial, relation):
        return self.sc.textFile('hdfs://localhost:9000/user/hadoop/proj4001/inputs/%s/%s'%(serial, relation))

    def outputData(self, serial, lines):
        try:
            p=subprocess.Popen('hadoop fs -mkdir -p proj4001/results/%s'%serial, shell=True, bufsize=4*1024, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            if p.wait()!=0:
                return False
        except:
            return False
        fname='proj4001/results/%s/part-00000'%serial
        try:
            f=subprocess.Popen("hadoop fs -put - %s"%fname, shell=True, bufsize=4*1024**2, stdin=subprocess.PIPE).stdin
            for line in lines:
                f.write(line)
                f.write('\n')
            f.close()
        except:
            return False
        else:
            return True

def getSparkContext(appname):
    sc=SparkContext("local", appname)
    sc.mixin=Mixin(sc)
    return sc

