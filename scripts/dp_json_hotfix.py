import json
import sys

def main(argv):
	dpFile = argv[1]
	outFile = argv[2]

	propertyMap = {}
	propertyMap["actors.CUSTOMER.RACE"] = "labels.RACE"
	propertyMap["actors.CUSTOMER.GENDER"] = "labels.GENDER"
	propertyMap["actors.CUSTOMER.INTEREST"] = "labels.INTEREST"
	propertyMap["actors.CUSTOMER.PERCEIVED QUALITY"] = "numbers.PERCEIVED QUALITY"
	propertyMap["actors.CUSTOMER.CAR"] = "labels.CAR"
	propertyMap["actors.CUSTOMER.MARITAL STATUS"] = "labels.MARITAL STATUS"
	propertyMap["actors.CUSTOMER.WILLINGNESS TO RECOMMEND"] = "numbers.WILLINGNESS TO RECOMMEND"
	propertyMap["actors.CUSTOMER.RELATIVE PERCEIVED QUALITY"] = "numbers.RELATIVE PERCEIVED QUALITY"
	propertyMap["actors.CUSTOMER.EDUCATION"] = "labels.EDUCATION"
	propertyMap["actors.CUSTOMER.ATTITUDE"] = "numbers.ATTITUDE"
	propertyMap["actors.CUSTOMER.AGE"] = "labels.AGE"
	propertyMap["actors.CUSTOMER.NPS"] = "numbers.NPS"
	propertyMap["actors.CUSTOMER.EMPLOYMENT"] = "labels.EMPLOYMENT"
	propertyMap["actors.CUSTOMER.PERCEIVED VALUE"] = "numbers.PERCEIVED VALUE"
	propertyMap["actors.CUSTOMER.INTENTIONS"] = "numbers.INTENTIONS"
	propertyMap["actors.CUSTOMER.PURCHASE INTENTIONS"] = "numbers.PURCHASE INTENTIONS"
	propertyMap["actors.CUSTOMER.SATISFACTION"] = "numbers.SATISFACTION"
	propertyMap["actors.CUSTOMER.TAG"] = "labels.TAG"
	propertyMap["actors.CUSTOMER.ACTIVITY"] = "labels.ACTIVITY"
	propertyMap["actors.CUSTOMER.INCOME"] = "labels.INCOME"

	i = 0
	
	with open(outFile, 'w') as outfile:
		with open(dpFile, 'rb') as f:
			for line in f:
				if i % 10000 == 0:
					print i

				jsonObj = json.loads(line)
				newjsonObj = {}

				for k,v in jsonObj.iteritems():
					key = k
					if k in propertyMap.keys():
						key = propertyMap[k]
					newjsonObj[key] = v

				outfile.write(json.dumps(newjsonObj)+"\n")

				i = i + 1

	outfile.close()

if __name__ == "__main__":
	main(sys.argv)