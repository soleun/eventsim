import csv
import logging
import re
import sys
import time
import unicodedata
import urllib2
import uuid

from dateutil.parser import parse

import ujson

logging.basicConfig(level=logging.WARN)

class Uri:
    def __init__(self, s):
        self.s = str(s)
    def toString(self):
        return "<"+str(self.s).lower().replace(" ","_")+">"

class Quad:
    def __init__(self, subject, predicate, object, context=None):
        self.s = subject
        if isinstance(predicate, Uri):
            self.p = predicate
        else:
            self.p = Uri(str(predicate))
        self.o = object
        self.c = context
        
    def toString(self):
        subject = self.s
        predicate = self.p.toString()
        object = ''
        
        if isinstance(self.s, Uri):
            subject = self.s.toString()
        
        if isinstance(self.o, (int, long, float)):
            object = str(self.o)
        elif isinstance(self.o, Uri):
            object = self.o.toString()
        elif isinstance(self.o, unicode):
            object = "\""+urllib2.unquote(str(unicodedata.normalize('NFKD', self.o).encode('ascii','ignore'))).replace('\\','').replace('\"','')+"\""
        else:
            try:
                object = u"\""+urllib2.unquote(str(self.o.encode('utf-8')).replace('\\','')).replace('\"','')+"\""
            except:
                print self.o
                print type(self.o)
        
        if self.c:
            return " ".join([subject, predicate, object, self.c])+" ."
        else:
            return " ".join([subject, predicate, object])+" ."

def write_customer(customer, customers, outfile):
    customer_id = customer["properties"]["customer_id"][0]["value"]
    customer_uri = Uri("/pointillist/actor/customer/"+customer_id)
    
    write_quad(Quad(customer_uri, "type", "customer"), outfile)
    write_quad(Quad(customer_uri, "customer_id", customer_id), outfile)
    
    for k, v in customer["properties"].iteritems():
        if k not in ["customer_id", "type"]:
            write_quad(Quad(customer_uri, "/pointillist/actor/properties/"+k, v[0]["value"]), outfile)
            
    if "outE" in customer.keys():
        for k, v in customer["outE"].iteritems():
            for edge in v:
                write_quad(Quad(customer_uri, "/pointillist/verb/"+k, Uri("/pointillist/event/"+edge["inV"])), outfile)

def write_event(event, outfile):
    event_id = event["id"]
    event_uri = Uri("/pointillist/event/"+event_id)
    
    write_quad(Quad(event_uri, "type", "event"), outfile)
    for k, v in event["properties"].iteritems():
        if k not in ["type"]:
            write_quad(Quad(event_uri, "/pointillist/event/properties/"+k, v[0]["value"]), outfile)
            
    if "outE" in event.keys():
        for k, v in event["outE"].iteritems():
            if k not in ["object"]:
                if k in ["next_type"]:
                    for edge in v:
                        write_quad(Quad(event_uri, "/pointillist/"+k+"/"+edge["properties"]["event_type"], Uri("/pointillist/event/"+edge["inV"])), outfile)
                else:
                    for edge in v:
                        write_quad(Quad(event_uri, "/pointillist/"+k, Uri("/pointillist/event/"+edge["inV"])), outfile)
                

def write_quad(q, outfile):
    outfile.write(q.toString()+"\n")

def write_events(outfile, previous_event, event_stack):
    eventType_per_customer = {}
    
    if previous_event != None:
        event_stack.append(previous_event)
        logging.debug("ADDING event: "+previous_event["id"])
    while event_stack:
        e = event_stack.pop()
        event_id = e["id"]
        event_time = e["properties"]["event_time"][0]["value"]
        if eventType_per_customer:
            e["outE"]["next_type"] = []
            for et_name, et in eventType_per_customer.iteritems():
                event_uuid = et["id"]
                duration = et["properties"]["event_time"][0]["value"] - event_time
                e["outE"]["next_type"].append({"inV": event_uuid, "id":str(uuid.uuid4()), "properties":{"duration": duration, "event_type": et_name}})
      
        #outfile.write(ujson.dumps(e)+"\n")
        write_event(e, outfile)
        eventType_per_customer[e["properties"]["event_type"][0]["value"]] = e
        logging.debug("WRITING event: "+e["id"])

def main(argv):
    eventTypes = {}
    events = {}
    previous_event = None
    previous_customer = None
    event_stack = []

    event_count = {}
    customers = set()

    i = 0
    customer_count = 0

    with open(argv[2], 'w') as outfile:
        with open(argv[1], 'rb') as f:
            for line in f:
                jsonObj = ujson.loads(line)
                
                
                # Adding Event
                event_uuid = str(uuid.uuid4())
                event_uri = Uri("/pointillist/event/"+event_uuid)
                customer_id = jsonObj["actors.CUSTOMER.CUSTOMER_ID"]
                customer_uri = Uri("/pointillist/actor/customer/"+jsonObj["actors.CUSTOMER.CUSTOMER_ID"])
                
                event_time = int(time.mktime(parse(jsonObj["eventTime"]).timetuple()))
                event_type = jsonObj["eventType"]
                event_type_uri = Uri("/pointillist/event_type/"+jsonObj["eventType"])
                
                event = {
                    "id": event_uuid,
                    "label": "event"
                }
                
                event_properties = {
                    "type": [{"value": "event", "id":str(uuid.uuid4())}],
                    "customer_id": [{"value": jsonObj["actors.CUSTOMER.CUSTOMER_ID"], "id":str(uuid.uuid4())}],
                    "event_type": [{"value": event_type, "id":str(uuid.uuid4())}],
                    "event_time": [{"value": event_time, "id":str(uuid.uuid4())}]
                }
                

                for k in jsonObj.keys():
                    key_split = k.split(".")
                    if len(key_split) >= 2 and key_split[0] in ["labels", "numbers"] and jsonObj[k] != "":
                        event_properties[key_split[1]] = [{"value":jsonObj[k], "id":str(uuid.uuid4())}]
                        #write_quad(Quad(event_uri, key_split[1], jsonObj[k]), outfile)

                event["properties"] = event_properties
                logging.debug("CREATING event object: "+event["id"])
                                             
                # event_type
                if event_type not in eventTypes.keys():
                    eventType_uuid = str(uuid.uuid4())
                    eventType = {
                        "id": eventType_uuid,
                        "label": "eventType"
                    }
                    eventType_properties = {
                        "type": [{"value": "event_type", "id":str(uuid.uuid4())}],
                        "name": [{"value": event_type, "id":str(uuid.uuid4())}]
                    }
                    eventType["properties"] = eventType_properties
                    #outfile.write(ujson.dumps(eventType)+"\n")

                    eventTypes[event_type] = eventType_uuid
                    #logging.debug("ADDING eventType: "+event_type)
                    
                    write_quad(Quad(event_type_uri, "type", "eventType"), outfile)
                else:
                    eventType_uuid = eventTypes[event_type]
                    #logging.debug("FOUND eventType: "+event_type)
                    
                event["outE"] = {}
                event["outE"]["object"] = [{"inV": eventType_uuid, "id":str(uuid.uuid4())}]
                write_quad(Quad(event_uri, "/pointillist/object", event_type_uri), outfile)
                #logging.debug("ADDING edge between event and eventType")
                                
                # Adding Customer
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

                    if previous_customer != None:
                        #outfile.write(ujson.dumps(previous_customer)+"\n")
                        if previous_customer["properties"]["customer_id"][0]["value"] not in customers:
                            write_customer(previous_customer, customers, outfile)
                            customers.add(previous_customer["properties"]["customer_id"][0]["value"])
                        #logging.debug("WRITING customer: "+str(previous_customer["properties"]["customer_id"]))
                        
                    previous_customer = customer
                    previous_customer_uuid = customer_uuid

                    write_events(outfile, previous_event, event_stack)
                    customer_count = customer_count + 1
                    previous_event = None
                else:
                    customer_uuid = previous_customer_uuid

                # Add edges to eventType
                if event_type not in customer["outE"].keys():
                    customer["outE"][event_type] = []
                customer["outE"][event_type].append({"inV": event_uuid, "properties": {"event_time": event_time, "event_type": event_type}, "id":str(uuid.uuid4())})
                
                #write_quad(Quad(customer_uri, "/pointillist/verb", event_uri), outfile)

                if previous_event != None:
                    duration = event_time - previous_event["properties"]["event_time"][0]["value"]
                    previous_event["outE"]["next"] = [{"inV": event_uuid, "id":str(uuid.uuid4()), "properties":{"duration": duration}}]
                    previous_event["properties"]["duration"] = [{"value": duration, "id":str(uuid.uuid4())}]

                    event_stack.append(previous_event)
                    logging.debug("ADDING event: "+previous_event["id"])
                previous_event = event

                i += 1

                if i % 10000 == 0:
                    print i
                # if customer_count == 933714:
                #     break
            write_events(outfile, previous_event, event_stack)
    outfile.close()

if __name__ == "__main__":
    main(sys.argv)
