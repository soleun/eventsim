import sys
import csv
import json
import uuid
from dateutil.parser import parse
import time

def num(s):
	try:
		return int(s)
	except ValueError:
		try:
			return float(s)
		except ValueError:
			return s

def main(argv):
	json_strings = []

	i = 1

	with open(argv[2], 'w') as outfile:
		with open(argv[1], 'rb') as f:
			reader = csv.DictReader(f, delimiter="\t")
			for line in reader:
				if i % 10000 == 0:
					print i

				event = {}
				for k, v in line.iteritems():
					if v != None and v != "":
						event[k] = v
				outfile.write(json.dumps(event)+"\n")

				i = i + 1
	outfile.close()

if __name__ == "__main__":
	main(sys.argv)