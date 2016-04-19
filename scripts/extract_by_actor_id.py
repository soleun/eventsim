import sys
import csv
import ujson
import uuid
from dateutil.parser import parse
import time

def main(argv):
    ids_to_extact = ["162284952", "162294043", "162294146"]

    i = 0

    with open(argv[2], 'w') as outfile:
        with open(argv[1], 'rb') as f:
            for line in f:
                jsonObj = ujson.loads(line)
                
                if jsonObj["actors.CUSTOMER.CUSTOMER_ID"] in ids_to_extact:
                    outfile.write(ujson.dumps(jsonObj)+"\n")

                i += 1

                if i % 10000 == 0:
                    print i
    outfile.close()

if __name__ == "__main__":
    main(sys.argv)