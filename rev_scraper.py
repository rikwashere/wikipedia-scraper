import requests
import json
import time
import csv
import os

sauce = 'https://nl.wikipedia.org/w/api.php?'
params = {	'action' : 'query',
			'rvlimit': 'max',
			'prop': 'revisions',
			'titles' : '',
			'rvprop': 'timestamp|user|userid|size|tags|flags|comment',
			'format': 'json'
		}

processed = os.listdir('revisions')

with open('top10k.csv', 'r') as csv_in:
	titles = csv.DictReader(csv_in)

	for title in titles:
		if title['page_id'] + '.json' in processed:
			print 'Already processed <%s>...' % title['page_title']
			continue

		t0 = time.time()
		print 'Requesting <%s>...' % title['page_title']

		params['titles'] = title['page_title']
		r = requests.get(sauce, params=params)

		revisions = json.loads(r.text)

		for page in revisions['query']['pages']:
			print 'Found %i revisions' % len(revisions['query']['pages'][page]['revisions'])

			for rev in revisions['query']['pages'][page]['revisions']:
				try:
					print '\t %s: %s' % (rev['timestamp'], rev['user'])
				except:
					print '\t Some encoding bullshit.'
		with open('revisions/%s.json' % title['page_id'], 'w') as json_out:
			json.dump(revisions['query'], json_out, sort_keys=True, indent=4, encoding='utf8')

		nap_time = time.time() - t0
		print 'Query completed in %.2f. Sleeping %.2f' % (nap_time, nap_time * 10)

		time.sleep(nap_time)