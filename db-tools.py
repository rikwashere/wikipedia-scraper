from collections import Counter
import pandas as pd
import pymongo

class Db():
	def __init__(self):
		self.client = pymongo.MongoClient()
		self.db = self.client.wikipedia
		self.revisions = self.db.revisions
		self.logs = self.db.logs
		
#		self.df = self.build_df()

	def build_df(self):
		cursor = self.revisions.find({})
		df = pd.DataFrame(list(cursor))
		del df['_id']
		df.index = pd.to_datetime(df.timestamp)
		self.df = df

	def get_stats(self):
		self.size_mb = self.db.command('dbstats')['dataSize'] / (1024 * 1024)
		print 'Database is %.2f MB' % self.size_mb

	def get_revisions(self):
		top_edited_pages = Counter([rev['title'] for rev in self.revisions.find()]).most_common(100)

		for top_edited_page, count in top_edited_pages:
			editors = len(Counter([rev['user'] for rev in self.revisions.find({'title':top_edited_page})]))
			total_bytes = 0

			print '<%s> was edited %i times, by %i different editors.' % (top_edited_page, count, editors)
			for revision in self.revisions.find({'title':top_edited_page}):
				print '\t %s by <%s> at <%s> (%i)' % (revision['type'], revision['user'], revision['timestamp'], (revision['newlen'] - revision['oldlen']))	
				total_bytes += (revision['newlen'] - revision['oldlen'])
			print 'Total mutation: %i bytes' % total_bytes


	def get_edits(self):
		edits_per_user = self.df['user'].value_counts().to_dict()

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

		
		self.u_pages_editted = len(self.df['title'].value_counts().to_dict())
		
		print '%i unique pages were editted, by %i editors.' % (self.u_pages_editted, len(self.df['user'].value_counts().to_dict()))

		for edit in edits:
			print '%s edits -> %i' % (edit, len(edits[edit]))

	def get_edits_user(self, user):
		return self.db.revisions.find({'user': user} )
	
if __name__ == '__main__':
	Database = Db()
