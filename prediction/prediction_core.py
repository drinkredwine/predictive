import sys
import os
from pyspark import SparkContext
from math import ceil, floor
import json
from time import time
from urllib import quote





def str_input_to_json_events(data):
    try:
        event = json.loads(data)
    except:
        return

    if event.get('type') == 'add_event' and event['data'].get('customer_id'):
        yield event['data']




def derive_attributes_churn(target_event_type):

    def extend_with_props_of_last_event(results, e):
        for prop_name, prop_value in e['properties'].iteritems():
            attribute = "lval_"+e['type']+"_"+prop_name
            results[attribute] = prop_value

    def remove_events_before_target(events_sorted):
        last_activity_ts = 0
        for i, e in enumerate(events_sorted):
            if e['type'] == 'campaign' and e['properties'].get('status') == 'delivered':
                last_activity_ts = e['timestamp']
                del events_sorted[:i]
                break
        return last_activity_ts

    def extend_attribute_counts(results, periods, e, days):
        for period in periods:
            attribute = "c{0}d_{1}".format(period, e['type'])

            if days < period:
                if attribute in results:
                    results[attribute] += 1
                else:
                    results[attribute] = 1

    def wrapped(customer):

        customer_id, events = customer
        last_activity = 0
        first_attributes = []

        events_sorted = sorted(events, key=lambda k: k['timestamp'], reverse=True)

        # find the first activity event and delete it together with all events before it
        last_activity_ts = remove_events_before_target(events_sorted)
        if last_activity_ts == 0 or len(events_sorted) == 0:
            return

        target_churn = 0 if last_activity_ts > time() - 7*24*3600 else 1
        results = {'customer_id': customer_id,
                   'target_event': target_churn,
                   'target_events_ts': last_activity}

        # time of the most recent event while customers was still active
        target_event_timestamp = events_sorted[0]['timestamp']

        # aggregate all remaining events
        for e in events_sorted:

            # skip unwanted events
            if e['type'] in ['campaign', 'Nightly']:
                continue

            days = (target_event_timestamp - e['timestamp']) / (60*60*24)

            #add counts of events in last [1,3,7,28] days
            extend_attribute_counts(results, [3], e, days)

            # add days from the last event (the first in reverse order, add first and block others)
            if e['type'] not in first_attributes:
                first_attributes.append(e['type'])
                results["dLast_{0}".format(e['type'])] = days

            # add days from the first event
            results["dFirst_{0}_days".format(e['type'])] = days

            #extend_with_props_of_last_event(results, e)

        yield json.dumps(results)

    return wrapped



def derive_attributes_churn_no_comment(target_event_type):

    def extend_with_props_of_last_event(results, e):
        for prop_name, prop_value in e['properties'].iteritems():
            attribute = "lval_"+e['type']+"_"+prop_name
            results[attribute] = prop_value

    def remove_events_before_target(events_sorted):
        for i, e in enumerate(events_sorted):
            if e['type'] == 'campaign' and e['properties'].get('status') == 'delivered':
                last_activity_ts = e['timestamp']
                del events_sorted[:i]
                break
        return last_activity_ts

    def extend_attribute_counts(results, periods, e, days):
        for period in periods:
            attribute = "c{0}d_{1}".format(period, e['type'])

            if days < period:
                if attribute in results:
                    results[attribute] += 1
                else:
                    results[attribute] = 1

    def wrapped(customer):

        customer_id, events = customer
        last_activity = 0
        first_attributes = []

        events_sorted = sorted(events, key=lambda k: k['timestamp'], reverse=True)

        last_activity_ts = remove_events_before_target(events_sorted)

        if last_activity_ts == 0 or len(events_sorted) == 0:
            return

        target_churn = 0 if last_activity_ts > time() - 7*24*3600 else 1
        results = {'customer_id': customer_id,
                   'target_event': target_churn,
                   'target_events_ts': last_activity}

        target_event_timestamp = events_sorted[0]['timestamp']

        for e in events_sorted:

            if e['type'] in ['campaign', 'Nightly']:
                continue

            days = (target_event_timestamp - e['timestamp']) / (60*60*24)
            extend_attribute_counts(results, [3], e, days)

            # if e['type'] not in first_attributes:
            #     first_attributes.append(e['type'])
            #     results["dLast_{0}".format(e['type'])] = days

            # results["dFirst_{0}_days".format(e['type'])] = days

            # extend_with_props_of_last_event(results, e)

        return json.dumps(results)

    return wrapped