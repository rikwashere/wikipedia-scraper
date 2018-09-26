from collections import Counter
import pandas as pd
import pymongo

def getStats(db):
	logs = db.logs
	revisions = db.revisions
	db_size = db.command('dbstats')['dataSize'] / (1024*1024)

	return 'Database contains %s logs and %s revisions.\nDatabase is %.2f MB.' % (logs.count(), revisions.count(), db_size)

def getRevisions(revisions):

	top_edited_pages = Counter([rev['title'] for rev in revisions.find()]).most_common(20)

	for top_edited_page, count in top_edited_pages:
		editors = len(Counter([rev['user'] for rev in revisions.find({'title':top_edited_page})]))
		
		print '<%s> was edited %i times, by %i different editors.' % (top_edited_page, count, editors)
		for revision in revisions.find({'title':top_edited_page}):
			print '\t %s by <%s> at <%s> (%i)' % (revision['type'], revision['user'], revision['timestamp'], (revision['newlen'] - revision['oldlen']))	
			 
def getActiveEditors(df):
	edits_per_user = df['user'].value_counts().to_dict()

	edits = {	'< 5' : [],
				'5 - 24' : [],
				'25 - 99' : [],
				'> 100' : []
				}

	for editor in edits_per_user:
		if edits_per_user[editor] < 5:
			edits['< 5'].append(editor)
		elif edits_per_user[editor] >= 5 and edits_per_user[editor] < 25:
			edits['5 - 24'].append(editor)
		elif edits_per_user[editor] >= 25 and edits_per_user[editor] < 100:
			edits['25 - 99'].append(editor)
		elif edits_per_user[editor] >= 100:
			edits['> 100'].append(editor)

	print '%i unique pages were editted.' % len(df['title'].value_counts().to_dict())
 
	for edit in edits:
		print '%s edits -> %i' % (edit, len(edits[edit]))


def readMongo(collection, query):
	cursor = collection.find(query)
	df = pd.DataFrame(list(cursor))
	del df['_id']

	df.index = pd.to_datetime(df.timestamp)
	return df

if __name__ == '__main__':
	client = pymongo.MongoClient()
	db = client.wikipedia
	
	print getRevisions(db.revisions)
	df = readMongo(db.revisions, {})
	getActiveEditors(df)