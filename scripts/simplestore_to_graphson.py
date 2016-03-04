import sys
import csv
import ujson
import uuid
from dateutil.parser import parse
import time

def main(argv):
    eventTypes = {}
    events = {}
    previous_event = None
    previous_customer = None
    event_stack = []


    event_count = {}

    i = 0

    with open(argv[2], 'w') as outfile:
        with open(argv[1], 'rb') as f:
            for line in f:
                jsonObj = ujson.loads(line)

                event_uuid = str(uuid.uuid4())
                event = {
                    "id": event_uuid,
                    "label": "event"
                }

                event_time = int(time.mktime(parse(jsonObj["eventTime"]).timetuple()))

                event_properties = {
                    "type": [{"value": "event", "id":str(uuid.uuid4())}],
                    "customer_id": [{"value": jsonObj["actors.CUSTOMER.CUSTOMER_ID"], "id":str(uuid.uuid4())}],
                    "event_type": [{"value": jsonObj["eventType"], "id":str(uuid.uuid4())}],
                    "event_time": [{"value": event_time, "id":str(uuid.uuid4())}]
                }

                for k in jsonObj.keys():
                    key_split = k.split(".")
                    if len(key_split) >= 2 and key_split[0] in ["labels", "numbers"] and jsonObj[k] != "":
                        event_properties[key_split[1]] = [{"value":jsonObj[k], "id":str(uuid.uuid4())}]

                event["properties"] = event_properties

                customer_id = jsonObj["actors.CUSTOMER.CUSTOMER_ID"]
                if previous_customer == None or previous_customer["properties"]["customer_id"][0]["value"] != customer_id:
                    customer_uuid = str(uuid.uuid4())
                    customer = {
                        "id": customer_uuid,
                        "label": "customer"
                    }
                    customer_properties = {
                        "type": [{"value": "customer", "id":str(uuid.uuid4())}],
                        "customer_id": [{"value": jsonObj["actors.CUSTOMER.CUSTOMER_ID"], "id":str(uuid.uuid4())}]
                    }
                    if "actors.CUSTOMER.SESSION_ID" in jsonObj.keys() and jsonObj["actors.CUSTOMER.SESSION_ID"] != "":
                        customer_properties["SESSION_ID"] = [{"value":jsonObj["actors.CUSTOMER.SESSION_ID"], "id":str(uuid.uuid4())}]
                    customer["properties"] = customer_properties
                    customer["outE"] = {}
                    customer["outE"]["verb"] = []

                    if previous_customer != None:
                        outfile.write(ujson.dumps(previous_customer)+"\n")
                    previous_customer = customer
                    previous_customer_uuid = customer_uuid

                    if previous_event != None:
                        event_stack.append(previous_event)
                    while event_stack:
                        e = event_stack.pop()
                        outfile.write(ujson.dumps(e)+"\n")
                    previous_event = None
                else:
                    customer_uuid = previous_customer_uuid

                eventType_id = jsonObj["eventType"]
                if eventType_id not in eventTypes.keys():
                    eventType_uuid = str(uuid.uuid4())
                    eventType = {
                        "id": eventType_uuid,
                        "label": "eventType"
                    }
                    eventType_properties = {
                        "type": [{"value": "event_type", "id":str(uuid.uuid4())}],
                        "name": [{"value": jsonObj["eventType"], "id":str(uuid.uuid4())}]
                    }
                    eventType["properties"] = eventType_properties
                    outfile.write(ujson.dumps(eventType)+"\n")

                    eventTypes[eventType_id] = eventType_uuid
                else:
                    eventType_uuid = eventTypes[eventType_id]


                # Add edges to eventType
                customer["outE"]["verb"].append({"inV": event_uuid, "properties": {"event_time": event_time, "event_type": eventType_id}, "id":str(uuid.uuid4())})

                event["outE"] = {}
                event["outE"]["object"] = [{"inV": eventType_uuid, "id":str(uuid.uuid4())}]

                if previous_event != None:
                    duration = event_time - previous_event["properties"]["event_time"][0]["value"]
                    previous_event["outE"]["next"] = [{"inV": event_uuid, "duration": duration, "id":str(uuid.uuid4())}]
                    previous_event["properties"]["duration"] = [{"value": duration, "id":str(uuid.uuid4())}]

                    event_stack.append(previous_event)
                previous_event = event

                i += 1

                if i % 10000 == 0:
                    print i
    outfile.close()

if __name__ == "__main__":
    main(sys.argv)