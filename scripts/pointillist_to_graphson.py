import sys
import csv
import json
import uuid
from dateutil.parser import parse
import time

def main(argv):
	customers = {}
	customer_events = {}
	eventTypes = {}
	eventType_events = {}
	events = {}
	events_array = []
	event_customers = {}
	event_eventTypes = {}

	i = 0

	with open(argv[1], 'rb') as f:
		for line in f:
			jsonObj = json.loads(line)
			
			customer_id = jsonObj["actors.CUSTOMER.CUSTOMER_ID"]
			if customer_id not in customers.keys():
				customer = {
					"id": str(uuid.uuid4()),
					"label": "customer"
				}
				customer_properties = {
					"type": [{"value": "customer", "id": str(uuid.uuid4())}],
					"customer_id": [{"value": jsonObj["actors.CUSTOMER.CUSTOMER_ID"], "id": str(uuid.uuid4())}],
					"auth": [{"value": jsonObj["labels.AUTH"], "id": str(uuid.uuid4())}],
					"http_method": [{"value": jsonObj["labels.HTTP_METHOD"], "id": str(uuid.uuid4())}],
					"http_status": [{"value": jsonObj["labels.HTTP_STATUS"], "id": str(uuid.uuid4())}],
					"level": [{"value": jsonObj["labels.USER_LEVEL"], "id": str(uuid.uuid4())}],
					"last_name": [{"value": jsonObj["actors.CUSTOMER.LAST_NAME"], "id": str(uuid.uuid4())}],
					"location": [{"value": jsonObj["location.LOCATION"], "id": str(uuid.uuid4())}],
					"race": [{"value": jsonObj["actors.CUSTOMER.RACE"], "id": str(uuid.uuid4())}],
					"gender": [{"value": jsonObj["actors.CUSTOMER.GENDER"], "id": str(uuid.uuid4())}],
					"interest": [{"value": jsonObj["actors.CUSTOMER.INTEREST"], "id": str(uuid.uuid4())}],
					"perceived_quality": [{"value": jsonObj["actors.CUSTOMER.PERCEIVED QUALITY"], "id": str(uuid.uuid4())}],
					"car": [{"value": jsonObj["actors.CUSTOMER.CAR"], "id": str(uuid.uuid4())}],
					"first_name": [{"value": jsonObj["actors.CUSTOMER.FIRST_NAME"], "id": str(uuid.uuid4())}],
					"marital_status": [{"value": jsonObj["actors.CUSTOMER.MARITAL STATUS"], "id": str(uuid.uuid4())}],
					"willingness_to_recommend": [{"value": jsonObj["actors.CUSTOMER.WILLINGNESS TO RECOMMEND"], "id": str(uuid.uuid4())}],
					"activity": [{"value": jsonObj["actors.CUSTOMER.ACTIVITY"], "id": str(uuid.uuid4())}],
					"relative_perceived_quality": [{"value": jsonObj["actors.CUSTOMER.RELATIVE PERCEIVED QUALITY"], "id": str(uuid.uuid4())}],
					"education": [{"value": jsonObj["actors.CUSTOMER.EDUCATION"], "id": str(uuid.uuid4())}],
					"attitude": [{"value": jsonObj["actors.CUSTOMER.ATTITUDE"], "id": str(uuid.uuid4())}],
					"age": [{"value": jsonObj["actors.CUSTOMER.AGE"], "id": str(uuid.uuid4())}],
					"nps": [{"value": jsonObj["actors.CUSTOMER.NPS"], "id": str(uuid.uuid4())}],
					"employment": [{"value": jsonObj["actors.CUSTOMER.EMPLOYMENT"], "id": str(uuid.uuid4())}],
					"perceived_value": [{"value": jsonObj["actors.CUSTOMER.PERCEIVED VALUE"], "id": str(uuid.uuid4())}],
					"intentions": [{"value": jsonObj["actors.CUSTOMER.INTENTIONS"], "id": str(uuid.uuid4())}],
					"satisfaction": [{"value": jsonObj["actors.CUSTOMER.SATISFACTION"], "id": str(uuid.uuid4())}],
					"income": [{"value": jsonObj["actors.CUSTOMER.INCOME"], "id": str(uuid.uuid4())}],
					"tag": [{"value": jsonObj["actors.CUSTOMER.TAG"], "id": str(uuid.uuid4())}]
				}
				customer["properties"] = customer_properties
				customers[customer_id] = customer
				customer_events[customer_id] = {}

			eventType_id = jsonObj["eventType"]
			if eventType_id not in eventTypes.keys():
				eventType = {
					"id": str(uuid.uuid4()),
					"label": "eventType"
				}

				eventType_properties = {
					"type": [{"value": "event_type", "id": str(uuid.uuid4())}],
					"name": [{"value": jsonObj["eventType"], "id": str(uuid.uuid4())}]
				}
				eventTypes[eventType_id] = eventType
				eventType_events[eventType_id] = []

			event_id = str(uuid.uuid4())
			event = {
				"id": event_id,
				"label": "event"
			}

			event_time = int(time.mktime(parse(jsonObj["eventTime"]).timetuple()))

			event_properties = {
				"type": [{"value": "event", "id": str(uuid.uuid4())}],
				"customer_id": [{"value": jsonObj["actors.CUSTOMER.CUSTOMER_ID"], "id": str(uuid.uuid4())}],
				"event_type": [{"value": jsonObj["eventType"], "id": str(uuid.uuid4())}],
				"event_time": [{"value": event_time, "id": str(uuid.uuid4())}]
			}

			event["properties"] = event_properties
			events[event_id] = event
			events_array.append(event_id)
			event_customers[event_id] = []
			event_eventTypes[event_id] = []

			customer_events[customer_id][event_id] = event
			eventType_events[eventType_id].append(event_id)
			event_customers[event_id].append(customer_id)
			event_eventTypes[event_id].append(eventType_id)

			i = i + 1

			if i % 10000 == 0:
				print i

	json_strings = []

	# Inserting events with "next" pointers.
	previous_event = {}
	for eid in events_array:
		e = events[eid]
		customer_id = e["properties"]["customer_id"][0]["value"]
		if customer_id in previous_event.keys():
			prev_event = previous_event[customer_id]
			prev_event["outE"] = {}
			duration = e["properties"]["event_time"][0]["value"]-prev_event["properties"]["event_time"][0]["value"]
			prev_event["outE"]["next"] = [{"inV": eid, "id": str(uuid.uuid4()), "duration": duration}]
			prev_event["properties"]["duration"] = [{"value": duration, "id": str(uuid.uuid4())}]

			json_strings.append(json.dumps(prev_event))
		
		previous_event[customer_id] = e

	# Inserting last events for each customers.
	for cid, e in previous_event.iteritems():
		json_strings.append(json.dumps(e))


	for cid, c in customers.iteritems():
		c["outE"] = {}
		c["outE"]["verb"] = []

		for eid, e in customer_events[cid].iteritems():
			evt = {"inV": eid, "id": str(uuid.uuid4()), "properties": {"event_time": e["properties"]["event_time"][0]["value"], "event_type": e["properties"]["event_type"][0]["value"]}}
			c["outE"]["verb"].append(evt)

		json_strings.append(json.dumps(c))

	for etid, et in eventTypes.iteritems():
		et["inE"] = {}
		et["inE"]["object"] = []

		for eid in eventType_events[etid]:
			e = {"outV": eid, "id": str(uuid.uuid4())}
			et["inE"]["object"].append(e)

		json_strings.append(json.dumps(et))

	with open(argv[2], 'w') as outfile:
	 	outfile.write("\n".join(json_strings))

if __name__ == "__main__":
	main(sys.argv)