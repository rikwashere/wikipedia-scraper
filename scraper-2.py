#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import requests
import pymongo
import random
import json
import time
import csv
import os

def scrape_api(data, source, params, data_keys):	
	r = requests.get(source, params=params)

	if r.status_code == 200:
		json_response = json.loads(r.text)
		changes = json_response['query']['recentchanges']

		for change in changes:
			
			if change.has_key('logid') and change['revid'] == 0:
				change['_id'] = change['logid']
				data['logs'][change['_id']] = change
	 		else:
				change['_id'] = change['revid']
				data['revisions'][change['_id']] = change

		return data
	else:
		time.sleep(2)
		return []

def getLastUpdate(logs, revisions):
	max_log = max(logs.find({}, {'_id': 1}))['_id']
	max_rev = max(revisions.find({}, {'_id': 1}))['_id']
	return max_rev, max_log

if __name__ == '__main__':

        if 'logs.csv' in os.listdir('.'):
            with open('logs.csv', 'a') as csv_out:
                writer = csv.writer(csv_out, delimiter='\t')
                writer.writerow([datetime.datetime.time(datetime.datetime.now()),
                                'REBOOT',
                                ])
        else:
	    with open('logs.csv', 'a') as csv_out:
		writer = csv.writer(csv_out, delimiter='\t')
		writer.writerow(['Time', 'New revs', 'New logs', 'Total revs', 'Total logs', 'Db size (mb)' ])

	# db stuff
	client = pymongo.MongoClient()
	db = client['wikipedia']
	
	data = {'revisions' : {},
			'logs' : {}
		}

	# load tables for logs and revisions
	logs_db = db.logs
	revisions_db = db.revisions
	max_rev, max_log = getLastUpdate(logs_db, revisions_db)

	# api stuff
	source = 'https://nl.wikipedia.org/w/api.php'
	data_keys = [ 	'user', 'userid', 'comment', 'parsedcomment', 'flags', 
					'timestamp', 'title', 'ids', 'sizes', 'redirect', 
					'loginfo', 'tags']

	params = { 	'action' : 'query',
				'list' : 'recentchanges',
				'format': 'json',
				'rcprop' : '|'.join(data_keys)}
	
	while True:
		#print 'Scraping...'
		t0 = time.time()
		data = scrape_api(data, source, params, data_keys)
		new_revs = []
		new_logs = []

		for rev_id in data['revisions']:
			if rev_id > max_rev:
				try:
					revisions_db.insert_one(data['revisions'][rev_id])
				except pymongo.errors.DuplicateKeyError:
					continue

				new_revs.append(rev_id)
		
		for log_id in data['logs']:	
			if log_id > max_log:		
				try:
					logs_db.insert_one(data['logs'][log_id])
				except pymongo.errors.DuplicateKeyError:
					continue

				new_logs.append(log_id)
		
		nap_time = (time.time() - t0) * random.randint(30,40)
		#print 'Database contains %i logs and %i revisions.' % (logs_db.count(), revisions_db.count())
		#print 'Crawled %i revisions and %i logs so far.' % (len(data['revisions']), len(data['logs']))
		#print '%s - Sleeping %.2f seconds.\n' % (datetime.datetime.time(datetime.datetime.now()), nap_time)
		

		max_rev, max_log = getLastUpdate(logs_db, revisions_db)

		data = {'revisions' : {},
				'logs' : {}
			}

		data_out = [	datetime.datetime.time(datetime.datetime.now()),
						len(new_revs),
						len(new_logs),
						logs_db.count(), 
						revisions_db.count(),
						'%.2f' % float(db.command('dbstats')['dataSize'] / (1024 * 1024))
					]

		with open('logs.csv', 'a') as csv_out:
			writer = csv.writer(csv_out, delimiter='\t')
			writer.writerow(data_out)

		time.sleep(nap_time)
