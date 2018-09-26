import pymongo

if __name__ == '__main__':
	client = pymongo.MongoClient()
	db = client.wikipedia
	logs = db.logs
	revisions = db.revisions
	db_size = db.stats(1024*1024)['size']

	print 'Database contains %s logs and %s revisions.\n Database is %.2f MB.' % (logs.count(), revisions.count(), db_size)