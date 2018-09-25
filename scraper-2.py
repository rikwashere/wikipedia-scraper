#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from bs4 import BeautifulSoup
import datetime
import requests
import sqlite3
import pymongo
import random
import json
import time
import sys
import re

def scrape_api(out, counter):
	source = 'https://nl.wikipedia.org/w/api.php'

	data_keys = [ 	'user', 'userid', 'comment', 'parsedcomment', 'flags', 
					'timestamp', 'title', 'ids', 'sizes', 'redirect', 
					'loginfo', 'tags']

	params = { 	'action' : 'query',
				'list' : 'recentchanges',
				'format': 'json',
				'rcprop' : '|'.join(data_keys)}
	
	r = requests.get(source, params=params)
	json_response = json.loads(r.text)
	changes = json_response['query']['recentchanges']

	for change in changes:
		# split revisions and changes
		counter += 1
		
		if change.has_key('logid') and change['revid'] == 0:
			change['_id'] = change['logid']
			out['logs_out'].append(change)
 		else:
			change['_id'] = change['revid']
			out['revisions_out'].append(change)

	return out, counter

def store(out, db):
	logs = db.logs
	revisions = db.revisions

	try:
		max_log = max(logs.find({}, {'_id', 1}))['_id']
		max_rev = max(revisions.find({}, {'_id', 1}))['_id']
	except:
		max_log = 0
		max_rev = 0

	# dit werk niet!
	logs.insert_many([c for c in out['logs_out'] if c['_id'] > max_log])
	revisions.insert_many([c for c in out['revisions_out'] if c['_id'] > max_rev])

	print 'Database has %i log and %i revisions.' % (logs.counter(), revisions.counter())

if __name__ == '__main__':
	client = pymongo.MongoClient()
	db = client['wikipedia']
	
	out = {	'revisions_out' : [],
			'logs_out' : []
		}
	counter = 0
	sleep = 5
	
	while True:
		print 'Scraping...'
		out, counter = scrape_api(out, counter)

		logs = db.logs
		revisions = db.revisions

		try:
			max_log = max(logs.find({}, {'_id': 1}))['_id']
			max_rev = max(revisions.find({}, {'_id': 1}))['_id']
		except:
			max_log = 0
			max_rev = 0

		new_logs = [c for c in out['logs_out'] if c['_id'] > max_log]
		new_revs = [c for c in out['revisions_out'] if c['_id'] > max_rev]

		if len(new_logs) > 0:
			print '\t-Inserting %i new log changes.' % len(new_logs)
			logs.insert_many(new_logs)
		else:
			print '\t-No new logs scraped.'

		if len(new_revs) > 0:
			print '\t-Inserting %i new revisions.' % len(new_revs) 
			revisions.insert_one(random.choice(new_revs))
			sleep -= 1
		else:
			print '\t-No new revisions scraped.'
			sleep += 1

		print 'Database has %i logs and %i revisions.' % (logs.count(), revisions.count())
		print '%s - Sleeping %.2f seconds.\n' % (datetime.datetime.time(datetime.datetime.now()), sleep)
		time.sleep(sleep)