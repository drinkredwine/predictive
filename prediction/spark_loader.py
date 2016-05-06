import os
import sys


class PrepareSpark:
    def __init__(self, appname="Full predictions"):
        self.set_environment()
        self.sc = self.init_sc(appname)
        self.sql_context=self.init_sql_context()

    def set_environment(self):
        os.environ['SPARK_HOME'] = '/Users/jozo/apps/spark-1.6.0'
        os.environ['PYTHONPATH'] = '/Users/jozo/apps/spark-1.6.0/python'
        spark_home = os.environ.get('SPARK_HOME', None)
        sys.path.insert(0, spark_home + "/python")  # pyspark
        sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.8.2.1-src.zip'))  # py4j

    def init_sc(self, appname):
        self.set_environment()
        from pyspark import SparkContext, SparkConf
        sc = SparkContext(
            master='local[6]', #master='spark://data-node001:7077',
            # pyFiles=['data_operations.py', 'prediction_core.py',
            #          'utils_analyses.py', 'spark_loader.py', 'training_core.py'],
            appName=appname
        )
        return sc

    def init_sql_context(self):
        from pyspark import SQLContext
        self.sql_context = SQLContext(self.sc)
        return self.sql_context

