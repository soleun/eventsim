import sys
import csv
import json
import uuid
from dateutil.parser import parse
import time
import logging
import requests

''' Example Payload

{
	"eventSource":"POS"
	"eventType":"Sale",
	"eventTime":"2015-9-24 7:6:23-0700",
	"actors":{
		"Customer":{
			"Loyalty_Card_Id":"1234567890",
			"Email_Id":"Myemail1@email.com",
			"Consumer_Id":"4893622"
		},
		"Product":{
			"Product_Id":"7680850106"
		},
		"Coupon":{
			"Coupon_Id":"C0001"
		},
		"Store":{
			"Store_Id":"1"
		},
		"Transaction":{
			"Sales_Id":"1"
		}
	},
	"labels":{
		"Sale_Type":"Store",
		"Payment_Method":"Cash",
		"Delivery_Mode":"In-store pick-up",
		"Product_Category":"Office Supplies",
		"Product_Sub_Category":"Storage & Organization",
		"Product_Name":"Eldon Base for stackable storage shelf"
	},
	"numbers":{
		"Sale_Amount":"80.77"
	},
	"location":{
		"Address_Street":"Street Address11",
		"countryCode":"US",
		"zipCode":"90069"
	}
}

'''

def on_error(error, items):
    print("An error occurred:", error)

def main(argv):
	print "apiToken = "+argv[1]
	headers = {'apiToken': argv[1]}
	datafile_to_push = argv[2]

	i = 1

	with open(datafile_to_push, 'rb') as f:
		for line in f:
			if i % 100 == 0:
				print i

			jsonObj = json.loads(line)

			payload = {}
			actors = {}

			payload["eventSource"] = "Sol's Laptop"
			payload["eventType"] = jsonObj["eventType"]
			payload["eventTime"] = jsonObj["eventTime"]
			payload["labels"] = {}
			payload["numbers"] = {}
			payload["location"] = {}

			for k, v in jsonObj.iteritems():
				parts = k.split(".")
				if len(parts) > 2:
					if parts[0].lower() == "actors":
						if parts[1].lower() not in actors.keys():
							actors[parts[1].lower()] = {}
						actors[parts[1].lower()][parts[2]] = v
				elif len(parts) == 2:
					if parts[0].lower() in ["labels", "numbers", "locations"]:
						payload[parts[0].lower()][parts[1].lower()] = v

			payload["actors"] = actors
			
			print payload["eventType"]
			print payload["eventTime"]
			print requests.post('https://stream.pointillist.com/event', data=json.dumps(payload), headers=headers)

			i = i + 1

if __name__ == "__main__":
	main(sys.argv)