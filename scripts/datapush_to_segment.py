import csv
import json
import sys
import time
import uuid

import analytics
from dateutil.parser import parse


def on_error(error, items):
    print("An error occurred:", error)

def main(argv):
    analytics.write_key = argv[1]
    analytics.debug = True
    print "analytics.write_key = "+argv[1]
    print "file to push = "+argv[2]
    datafile_to_push = argv[2]

    i = 1

    with open(datafile_to_push, 'rb') as f:
        for line in f:
            jsonObj = json.loads(line)

            user_id = jsonObj["actors.CUSTOMER.CUSTOMER_ID"]
            event_type = jsonObj["eventType"]
            event_time = parse(jsonObj["eventTime"])
            user_properties = {}

            for k, v in jsonObj.iteritems():
                if "actors.CUSTOMER" in k:
                    user_properties[k.replace("actors.CUSTOMER.", "")] = v

            analytics.identify(user_id, user_properties)

            event_properties = {}

            for k, v in jsonObj.iteritems():
                key_type = k.split(".")[0]
                if key_type in ["labels", "numbers", "locations"]:
                    event_properties[k.replace(key_type+".", "")] = v

            analytics.track(user_id, event_type, event_properties, integrations={"all":True, "Pointillist":True}, timestamp=event_time)

            i = i + 1
            
            if i % 10000 == 0:
                print i

    analytics.flush()

if __name__ == "__main__":
    main(sys.argv)
