from pyspark.mllib.regression import LabeledPoint
# import numpy

#
# def predict_model_from_model(node):
#     def predict_node(data):
#
#         return predict_impl(node, data)
#
#     def predict_impl(node, features):
#         data = features.features
#         if node.isLeaf():
#             return node.id()
#         if data[node.split.get.feature] <= node.split.get.threshold:
#             predict_impl(node.leftNode.get, data)
#         else:
#             predict_impl(node.rightNode.get, data)
#
#     return predict_node
#
#
# def predict_model_recursive(node):
#
#     def predict_node(features):
#         return predict_impl(node, features)
#
#     def predict_impl(node, features):
#         if not node.get('children'):
#             return node['node_id']
#         if features[node['attribute_id']] <= node['threshold']:
#             return predict_impl(node['children'][0], features)
#         else:
#             return predict_impl(node['children'][1], features)
#
#     return predict_node
#
# def update_tree(tree, leaves):
#     return True


def fill_tree(node, leaves):
    if not node.get('children'):
        node['good'] = leaves[node['node_id']].get(0,0)
        node['bad'] = leaves[node['node_id']].get(1,0)
        return node

    left = fill_tree(node['children'][0], leaves)
    right = fill_tree(node['children'][1], leaves)

    node['good'] = left['good'] + right['good']
    node['bad'] = left['bad'] + right['bad']

    return node


def predict_model(node):

    def predict_node(lp):
        subnode = node
        while subnode.get('children'):
            if lp.features[subnode['attribute_id']] <= subnode['threshold']:
                subnode = subnode['children'][0]
            else:
                subnode = subnode['children'][1]
        return subnode['node_id'], lp.label, 1

    return predict_node


def convert_node( node, features, rules, attribute=None):

    if node.isLeaf():
        return {
            'node_id': node.id(),
            'attribute': attribute,
            'rules': rules,
            'prediction': node.predict().predict(),
            'good': 0,
            'bad': 0
        }

    split = node.split().get()
    original_attribute = attribute
    attribute = features[split.feature()]
    threshold = split.threshold()

    children = [
        convert_node(node.leftNode().get(), features, '{}<={}'.format(attribute, threshold), attribute),
        convert_node(node.rightNode().get(), features, '{}>{}'.format(attribute, threshold), attribute)
    ]

    return {
        'node_id': node.id(),
        'attribute_id': split.feature(),
        'attribute': original_attribute,
        'threshold': threshold,
        'prediction': node.predict().predict(),
        'good': 0,
        'bad': 0,
        'rules': rules,
        'children': children
    }


def convert_model(model, features):
    return convert_node(model.call('topNode'), features, '')


def get_features_from_df_row(df):
    i = 0
    features = {}
    for column, v in df.first().asDict().iteritems():
        match = False
        for c in ['target_event', 'customer_id', 'purchase' ]:
            if column.find(c) >= 0:
                match = True
                break
        if is_number(v) and not match:
            features[i] = column
            i += 1
    return features


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def label_points(target):
    def wrapped(row):
        label = row[target]
        features = []
        attributes = []
        for k, v in row.asDict().iteritems():
            match = False
            for c in [target,'customer_id', 'purchase' ]:
                if k.find(c) >= 0:
                    match = True
                    break
            if is_number(v) and not match:
                if type(v) in [float, int]:
                    features.append(float(v))
                    attributes.append(str(k))
        return LabeledPoint(label, features)
    return wrapped

def replace_tree(model, features):
    import re
    tree=""
    for row in str(model.toDebugString()).split('\n'):
        if len(re.findall("(feature) (\d*)",row) ) > 0:
            f,n = re.findall("(feature) (\d*)",row)[0]
            out = re.sub("feature \d*"," "+features[int(n)],row)
        else:
            out = row
        tree+="\n"+out
    return tree