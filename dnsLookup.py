import requests
import json
import time

def lookup(target):
	t0 = time.time()
	end_point = 'https://rest.db.ripe.net/search.json'
	
	params = {	'query-string' : target, 
				'flags' : 'no-filtering',
				'source': 'RIPE'
			}
	
	response = requests.get(end_point, params=params)

	data = json.loads(response.text)

	return process(data)

def process(response):
	attrs = [attrs['attributes']['attribute'] for attrs in response['objects']['object']]
	output = {}
	
	for attrz in attrs:
		for attr in attrz: 
			output[attr['name']] = attr['value']
				
	ip_range = output['inetnum']
	low, high = ip_range.split(' - ')
	
	output['ip_range'] = {	'low' : low,
							'high' : high
						}
	
	return output


if __name__ == '__main__':
	target = raw_input('Lookup what ip?\n> ')
	dns_info = lookup(target)
	
	