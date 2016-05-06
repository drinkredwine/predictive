def get_string_rules(rows, total, column):
    # bin 0 is default
    # all categories with size > 5% will have number 1,2,3,..
    # special case: others can have less than 5%, if so then are joined with smallest bin
    unassigned = total
    bin = 1
    rules_inv = {}

    for i, row in enumerate(rows):
        if row['count'] >= 0.05 * total:
            rules_inv[row[column]] = bin
            unassigned -= row['count']
            bin += 1

    if float(unassigned) / total > 0.05:
        rules_inv['<others>'] = 0
    else:
        rules_inv['<others>'] = bin - 1

    return rules_inv


def get_numeric_rules(rows, total, column):
    # bin 0 is default
    # all categories with size > 5% will have number 1,2,3,..
    # special case: others can have less than 5%, if so then are joined with smallest bin
    smallest_bin_perc = 0.20
    unassigned = total
    current_bin = 0
    bin = 1
    rules_inv = {}
    last_boundary = -float("inf")

    for i, row in enumerate(rows):
        # null values
        if row[column] is None:
            unassigned -= row['count']
            if row['count'] >= total * smallest_bin_perc:
                rules_inv[("none")] = -1
            else:
                rules_inv[("none")] = bin
            continue

        # fill the bin
        current_bin += row['count']
        if current_bin >= total * smallest_bin_perc:
            rules_inv[(last_boundary, row[column])] = bin
            last_boundary = row[column]
            unassigned -= current_bin
            current_bin = 0
            bin += 1

    rules_inv[(row[column], float("inf"))] = bin
    unassigned -= current_bin

    if unassigned > 0:
        print "!!!!!!!unassigned:", unassigned

    return rules_inv


def transform(rules):
    def lookup_rule(val, column, rule):
        out = -1
        if rule:
            # string
            if val in rule:
                return rule[val]
            # number
            for orig, target in rule.iteritems():
                if len(orig) == 2 and orig[0] < val <= orig[1]:
                    return target
            # string default
            if '<others>' in rule:
                return rule['<others>']
            # number default
            for orig, target in rule.iteritems():
                if orig == 'none':
                    return target
        return out

    def wrapped(row):
        out = {}

        for col_name, col_val in row.asDict().iteritems():
            out[col_name] = lookup_rule(col_val, col_name, rules.get(col_name))
        return out

    return wrapped

def stratified_sampling(dataset, target_counts, target, max_occurences=10000.0):
    """
    :param dataset: dataframe
    :param target_counts: dictionary with target value and count of obesrvations, e.g. {0: 100, 1:150}
    :param max_occurences: max number of target value occurences in final dataset
    :return: sampled dataset
    """
    fractions={}

    for ttarget, count in target_counts.iteritems():
        fractions[ttarget] = min(1.0, max_occurences / count)

    dataset_sampled = dataset.sampleBy(target, fractions=fractions, seed=0)

    return dataset_sampled


def count_good_and_bads(dataset, target):
    """
    :param dataset: r
    :return: dict with target_value: count pairs, {0: 100, 1:150}
    """
    query = dataset.groupBy([target]).count().collect()

    target_stats = {}
    for row in query:
        target_stats[row.asDict()[target]] = row.asDict()['count']

    return target_stats


def evaluate_model(model, dataset):
    predictions = model.predict(dataset.map(lambda x: x.features))
    labelsAndPredictions = dataset.map(lambda x: x.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(dataset.count())
    return testErr
