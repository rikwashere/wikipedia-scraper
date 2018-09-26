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