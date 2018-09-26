from collections import Counter
import pymongo

def getStats(db):
	logs = db.logs
	revisions = db.revisions
	db_size = db.command('dbstats')['dataSize'] / (1024*1024)

	return 'Database contains %s logs and %s revisions.\nDatabase is %.2f MB.' % (logs.count(), revisions.count(), db_size)

def getRevisions(revisions):

	top_edited_pages = Counter([rev['title'] for rev in revisions.find()]).most_common(20)

	for top_edited_page, count in top_edited_pages:
		print '<%s> was edited %i times.' % (top_edited_page, count)

                for revision in revisions.find({'title':top_edited_page}):
			print '\t Revision by <%s> at <%s>' % (revision['user'], revision['timestamp'])

if __name__ == '__main__':
	client = pymongo.MongoClient()
	db = client.wikipedia
	
	print getRevisions(db.revisions)
