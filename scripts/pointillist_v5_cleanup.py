import sys
import csv
import ujson
import uuid
from dateutil.parser import parse
import time
import random

def main(argv):
    events_with_customer_data = ["Set Customer Profile"]
    events_with_product_data = ["Product Searched", "Return in Store", "Add To Cart", "Bar Code Scanned", "Checkout", "Product Compared", "Product Rated", "Product Recommended", "Product Reviewed", "Product Shared", "Product Viewed"]
    
    event_attributes_to_supress = ["auth", "http_method", "http_status", "user_level", "item_in_session"]
    customer_attributes_to_supress = ["customer_id", "last_name", "location", "race", "gender", "interest", "perceived_quality", "car", "first_name", "marital_status", "willingness_to_recommend", "activity", "relative_perceived_quality", "education", "attitude", "age", "nps", "perceived_value", "intentions", "satisfaction", "income"]
    
    product_data = []
    
    with open(argv[2], 'rb') as pf:
        for line in pf:
            productjsonObj = ujson.loads(line)
            product_data.append(productjsonObj)
    
    i = 0
    
    with open(argv[3], 'w') as outfile:
        with open(argv[1], 'rb') as f:
            for line in f:
                jsonObj = ujson.loads(line)
                eventType = jsonObj["eventType"]
                
                if eventType in events_with_customer_data:
                    for k in jsonObj.keys():
                        if len(k.split(".") >= 2):
                            key = k.split(".")[1].lower()
                            if key in customer_attributes_to_supress:
                                jsonObj.pop(k, None)
                        
                if eventType in events_with_product_data:
                    product = product_data[random.randint(0, len(product_data))]
                    
                    for k in ["title", "price", "brand", "imUrl", "categories"]:
                        if k in product.keys():
                            if k == "categories":
                                if product["categories"] and product["categories"][0]:
                                    for i in xrange(1, 6):
                                        if product["categories"][0][i]:
                                            jsonObj["product_category_"+str(i)] = product["categories"][0][i]
                            else:
                                jsonObj["product_"+k] = product[k]
                    
                for k in jsonObj.keys():
                    if len(k.split(".") >= 2):
                        key = k.split(".")[1].lower()
                        if key in event_attributes_to_supress:
                            jsonObj.pop(k, None)
                
                outfile.write(ujson.dumps(jsonObj)+"\n")
                
                i = i + 1
                if i % 10000 == 0:
                    print i
    outfile.close()

if __name__ == "__main__":
    main(sys.argv)