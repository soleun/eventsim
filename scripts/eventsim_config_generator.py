import sys
import csv
import json

def getBasicConf(nodes):
	# Adding Basic Conf
	conf = {}

	conf['seed'] = 1
	conf['alpha'] = float(60*60) # Baseline time between transition is 1 hour
	conf['beta'] = 518400.0
	conf['damping'] = 0.09425
	conf['weekend-damping'] = 0.53
	conf['weekend-damping-offset'] = 180
	conf['weekend-damping-scale'] = 360
	conf['session-gap'] = 1800
	conf['churned-state'] = 'Cancelled'
	conf['show-user-details'] = [
		{"auth" : "Guest", "show" : True},
		{"auth" : "Logged In", "show" : True},
		{"auth" : "Logged Out", "show" : False},
		{"auth" : "Cancelled", "show" : True}
	]
	conf['levels'] = [{"level":"prospect","weight":10}]#, {"level":"customer","weight":2}]
	conf['auths'] = [{"auth":"Guest","weight":10}]#, {"auth":"Logged In","weight":10}, {"auth":"Logged Out","weight":1}]

	conf['new-session'] = generateNewSessionEvents(nodes)

	conf['new-user-auth'] = "Guest"
	conf['new-user-level'] = "prospect"
	conf['upgrades'] = []
	conf['downgrades'] = []

	return conf

def generateNewSessionEvents(nodes):
	'''
	{
      "status": 200,
      "weight": 20,
      "level": "prospect",
      "auth": "Guest",
      "method": "GET",
      "page": "Advertising Presented"
    },
    '''
	newsession = []

	for n in nodes:
		if n['new session'] > 0:
			newsession.append({'status':200, 'weight':n['new session'], 'level':'prospect', 'auth':'Guest', 'method':'GET', 'page':n['name']})

	return newsession

def getEvent(name, method, status, auth, level):
	return {'page':name, 'method':method, 'status':status, 'auth':auth, 'level':level}

def parseNodes(path):
	nodes = []
	with open(path, 'rb') as f:
		reader = csv.reader(f)
		next(reader, None)
		for row in reader:
			nodes.append({'name':row[0], 'event type':row[1], 'type':row[2], 'subtype':row[3], 'details':row[4], 'new session':int(row[9])})

	return nodes

def parseEdges(path):
	edges = []
	with open(path, 'rb') as f:
		reader = csv.reader(f)
		next(reader, None)
		for row in reader:
			edges.append({'source':row[0], 'dest':row[1], 'p':float(row[2]), 't':float(row[3])})

	return edges

def generateTransitions(edges):
	transitions = []

	for e in edges:
		transitions.append({'source':getEvent(e['source'],'GET',200,'Guest','prospect'), 'dest':getEvent(e['dest'],'GET',200,'Guest','prospect'), 'p':e['p'], 't':e['t']})

	return transitions

def main(argv):
	nodes = parseNodes(argv[1])
	edges = parseEdges(argv[2])

	conf = getBasicConf(nodes)
	conf['transitions'] = generateTransitions(edges)

	with open(argv[3], 'w') as outfile:
		json.dump(conf, outfile)

if __name__ == "__main__":
	main(sys.argv)