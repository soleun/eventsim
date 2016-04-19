import json
import sys

def main(argv):
	productFile = argv[1]
	outFile = argv[2]

	i = 0
	cnt = 0
	with open(outFile, 'w') as outfile:
		with open(productFile, 'rb') as f:
			for line in f:
				if i % 10000 == 0:
					print i

				jsonObj = json.loads(line)

				if "salesRank" in jsonObj.keys():
					for k, v in jsonObj["salesRank"].iteritems():
						if v < 1000:
							outfile.write(json.dumps(jsonObj)+'\n')
							cnt = cnt + 1
							break

				i = i + 1

	print cnt
	outfile.close()

if __name__ == "__main__":
	main(sys.argv)