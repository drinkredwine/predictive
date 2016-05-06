import os
from urllib import quote
import json
from math import floor


def get_sources(conf, events):
    src = []

    for e in events:
        dir_name = os.path.join(conf['data_dir'], conf['instance'], conf['company'], 'add_event', quote(e))
        if os.path.exists(dir_name):
            src.append(os.path.join(dir_name, conf['date'], "*.json"))

    return ",".join(src)


def load_valid_events(txt):
    try:
        event = json.loads(txt)
    except:
        return

    if event.get('type') == 'add_event' and event['data'].get('customer_id'):
        yield event['data']


def derive_attributes(target_event_type):
    def wrapped(customer):

        customer_id, events = customer
        target_event = 0
        events_count = 0
        first_attributes = []
        PERIOD_GENERATED_SECS = [60*60*24]

        # search for target event from beginning
        events = sorted(events, key=lambda k: k['timestamp'], reverse=False)

        for e in events:
            events_count += 1
            target_event_timestamp = e['timestamp']

            if e['type'] == target_event_type:
                target_event = 1
                del events[events_count:]
                break

        events = sorted(events, key=lambda k: k['timestamp'], reverse=True)

        results = {'customer_id': customer_id,
                   'target_event': target_event,
                   'target_events_ts': target_event_timestamp}

        for e in events:
            if e['type'] == target_event_type:
                continue

            days = floor((target_event_timestamp - e['timestamp']) / (60*60*24))
            mins = floor((target_event_timestamp - e['timestamp']) / 60)
            secs = floor((target_event_timestamp - e['timestamp']))

            for period in [PERIOD_GENERATED_SECS]:
                attribute = "count_"+e['type']+"_"+str(period)+'s'
                if secs < period:
                    if attribute in results:
                        results[attribute] += 1
                    else:
                        results[attribute] = 1

            if e['type'] not in first_attributes:
                first_attributes.append(e['type'])
                attribute = "last_"+e['type']+"_secs"
                results[attribute]=secs

            attribute = "first_"+e['type']+"_secs"
            results[attribute]=secs

        return json.dumps(results)
    return wrapped
