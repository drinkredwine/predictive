from pyspark import SparkContext, SQLContext, SparkConf

import json
import os

from urllib import quote
from prediction.prediction_core import str_input_to_json_events, derive_attributes, derive_attributes_churn

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree

from prediction.utils_analyses import label_points, get_features_from_df_row, replace_tree, convert_model, convert_node
from prediction.training_core import (
    count_good_and_bads, stratified_sampling, evaluate_model, get_numeric_rules, get_string_rules, transform)
from prediction.utils_analyses import predict_model

from prediction.core import get_sources, load_valid_events

conf = {
    "instance": "CIN",
    "company": "p1",
    "target_event": "payment",
    "allowed_events": ['session_start', 'payment'],
    "date": "2016*",
    "data_dir": "data"
}

# make sure target isn't in allowed_events
allowed_events = [x for x in conf['allowed_events'] if x != conf['target_event'] ]

sc = SparkContext(master='local[*]',appName='predictive')

for e in allowed_events:
    src = get_sources(conf, [e, conf['target_event']])
    customers = sc.textFile(src)\
        .flatMap(load_valid_events)\
        .groupBy(lambda x: x['customer_id'])\
        .map(derive_attributes(conf['target_event']))
    print customers.count()

exit()



sql_context = SQLContext(sc)

#dataset = spark.sql_context.createDataFrame(customers)
dataset = sql_context.read.json("numeric.json")

schema = dataset.schema.jsonValue().get('fields')
total = dataset.count()

offset = 0
rules_dict = {}

for i, column in enumerate(dataset.columns):
    j = i + offset
    if schema[j]['type'] in ['string']:
        stats = dataset.groupby(column).count().sort("count", ascending=False).take(10)
        rules = get_string_rules(stats, total, column)
    else:
        stats = dataset.groupby(column).count().sort(column, ascending=True).collect()
        rules = get_numeric_rules(stats, total, column)
    rules_dict[column] = rules

#print rules_dict
transformed = dataset.map(transform(rules_dict))
#print transformed.first()

target_orig = 'target'
target = 'target_event'
spark.sql_context.registerDataFrameAsTable(dataset, 'dataset')

dataset_2 = spark.sql_context.sql(
    """select *, case when {0}>=1 then 1 else 0 end as {1}
         from dataset""". format(target_orig, target))

target_counts = count_good_and_bads(dataset_2.select(target), target)
#print target_counts
dataset_sampled = stratified_sampling(dataset_2, target_counts, target, 10000.0)
#print dataset_sampled.first()
#dataset_fill_na = dataset_sampled.na.fill(0).select("*")

dataset_labeled_points = dataset_sampled.map(label_points(target))
features = get_features_from_df_row(dataset_sampled)
trainData, testData = dataset_labeled_points.randomSplit([0.5, 0.5])

# print trainData.first()
# model = DecisionTree.trainRegressor(trainData, categoricalFeaturesInfo={},
#                                     impurity='variance', maxDepth=5, maxBins=10,
#                                     minInstancesPerNode=100)
# print model
# from pyspark.ml.classification import LogisticRegression
#
# lr = LogisticRegression(maxIter=5, regParam=0.01)
# model_lr = lr.fit(trainData.toDF())
# print model_lr.coefficients
# print model_lr.intercept

#
# fst = trainData.first()
# predict = mordel.predict(trainData.first())
# print predict
model = DecisionTree.trainClassifier(
    trainData, numClasses=2, categoricalFeaturesInfo={},impurity='gini', maxDepth=5, maxBins=32, minInfoGain=0.01)

#evaluate_model(model, dataset)

model_d3_json = convert_model(model, features)
model_human_readlable = replace_tree(model, features)

# print(json.dumps(model_d3_json))
# print model_human_readlable

scored_rdd = dataset_labeled_points.map(predict_model(model_d3_json))
scored_df = spark.sql_context.createDataFrame(scored_rdd)
spark.sql_context.registerDataFrameAsTable(scored_df, "prediction")

query_df = spark.sql_context.\
    sql("select _1 as node_id, "
        "       sum(_2) as good, "
        "       count(*) as total "
        "  from prediction "
        " group by _1 "
        " order by _1 ").collect()

leaves = {}
for row in query_df:
    print row
    leaves[row.node_id] = {1: row.asDict()['good'], 0: row.asDict()['total']-row.asDict()['good']}

# print leaves
# print json.dumps(leaves)

from utils_analyses import fill_tree
model_d3_json_final = fill_tree(model_d3_json, leaves)

print model_d3_json_final
print(json.dumps(model_d3_json_final))


#tree={"threshold": 0.0, "attribute": "count_Store - click buy button_3d", "attribute_id": 15, "children": [{"rules": "count_Store - click buy button_3d<=0.0", "attribute": "count_Store - click buy button_3d", "predict": 0.0, "node_id": 2}, {"threshold": 1633628.0, "attribute": "first_campaign_secs", "attribute_id": 46, "children": [{"rules": "first_campaign_secs<=1633628.0", "attribute": "first_campaign_secs", "predict": 0.9056122448979592, "node_id": 6}, {"rules": "first_campaign_secs>1633628.0", "attribute": "first_campaign_secs", "predict": 0.0, "node_id": 7}], "rules": "count_Store - click buy button_3d>0.0"}], "rules": ""}
#
# destination = "dataset/churn.json"
# dataset = spark.sql_context.read.load(destination, format="json")
# spark.sql_context.createDataFrame(transformed)\
#     .write.format('json')\
#     .save("dataset.json")
#
# print rules_dict
# try:
#     with open('rules.json', 'w') as outfile:
#         json.dump(rules_dict, outfile)
# except:
#     pass
