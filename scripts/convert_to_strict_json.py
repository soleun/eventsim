import json
import gzip
import sys

def parse(path):
	g = gzip.open(path, 'r')
	for l in g:
		yield json.dumps(eval(l))

f = open("output.strict", 'w')
for l in parse(sys.argv[1]):
	f.write(l + '\n')