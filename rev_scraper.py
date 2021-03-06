import pandas as pd
import requests
import json
import time
import csv
import sys
import os

def log(txt):
	with open('logs.txt', 'ab') as text_out:
		text_out.write(txt)
		text_out.write('\n')

def crawl_revisions_page(file):
	sauce = 'https://nl.wikipedia.org/w/api.php?'
	params = {	'action' : 'query',
					'rvlimit': 'max',
					'prop': 'revisions',
					'titles' : '',
					'rvprop': 'timestamp|user|userid|size|tags|flags|comment',
					'format': 'json'
			}

	with open(file, 'r') as csv_in:
		titles = csv.DictReader(csv_in)

		for title in titles:
#			if title['page_id'] + '.json' in processed:
#				log('Already processed <%s>...' % title['page_title'])
#				continue

			t0 = time.time()
			log('Requesting <%s>...' % title['page_title'])

			params['titles'] = title['page_title']
			r = requests.get(sauce, params=params)

			revisions = json.loads(r.text)

			for page in revisions['query']['pages']:
				log('Found %i revisions' % len(revisions['query']['pages'][page]['revisions']))

				for rev in revisions['query']['pages'][page]['revisions']:
					try:
						log('\t %s: %s' % (rev['timestamp'], rev['user']))
					except:
						log('\t Some encoding bullshit.')
			with open('revisions/%s.json' % title['page_id'], 'w') as json_out:
				json.dump(revisions['query'], json_out, sort_keys=True, indent=4, encoding='utf8')

			nap_time = time.time() - t0
			log('%s Query completed in %.2f. Sleeping %.2f' % (time.time(), nap_time, nap_time * 10))

			time.sleep(nap_time)

def crawl_revisions_user(user):
	sauce = "https://en.wikipedia.org/w/api.php"
	params = {	'action' : 'query',
				'list' : 'allrevisions',
				'arvuser' : user_name,
				'arvlimit': 'max',
				'arvprop': 'ids|flags|timestamp|user|userid|size|tags',
				'format': 'json'
			}

	r = requests.get(sauce, params=params)

	response = json.loads(r.text)
	revisions = response['query']['allrevisions']
	print 'Found %i revisions for %s' % (len(revisions), user)
	json.dump(revisions, open('data/%s.json' % user, 'w'), indent=4, sort_keys=True)




if __name__ == '__main__':
	target = raw_input('Load which file?\n> ')
	df = pd.read_csv(target)

	for user_name in df['user_name']:
		crawl_revisions_user(user_name)