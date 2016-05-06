#!/bin/bash

source /Users/jozo/predictive_venv/bin/activate

if [ -f .files.zip ]; then
  rm files
fi

zip -r files.zip prediction predictive.py

export SPARK_HOME=/Users/jozo/apps/spark-1.6.0
export PYTHONPATH=$PYTHONPATH:/Users/jozo/apps/spark-1.6.0/python:/Users/jozo/apps/spark-1.6.0/python/lib/py4j-0.9-src.zip
export PATH=$PATH:$SPARK_HOME/python:$SPARK_HOME/bin

echo $PATH
echo $SPARK_HOME


python predictive.py