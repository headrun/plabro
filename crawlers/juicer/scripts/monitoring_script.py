import MySQLdb
import traceback
import datetime
from datetime import timedelta
from utils import *

class MonitoringScript(Utils):
	def __init__(self):
		Utils.__init__(self)

		self.db 	= "REAL_ESTATES"
		self.host 	= "localhost"
		self.user 	= "root"

	def collect_stats(self):
		try:
			one_hour = (datetime.datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
			message = "%s - %s" %("MagicBricks", one_hour)	
			stats_dict = {}


			self.open_cursor(self.host, self.db)
			query = 'select count(*) from magicbricks'
			self.cursor.execute(query)
			records = self.cursor.fetchall()
			if records:
				msg = "Number of records"
				stats_dict[msg] = records[0][0]

			query = 'select count(*) from magicbricks where contact_number = "" and created_at > "%s"' %one_hour
			self.cursor.execute(query)
			records = self.cursor.fetchall()
			if records:
				msg = "Number of records not having Phone number created last hour"
				stats_dict[msg] = records[0][0]

			query = 'select count(*) from magicbricks where contact_number = ""'
			self.cursor.execute(query)
			records = self.cursor.fetchall()
			if records:
				msg = "Number of records not having Phone number"
				stats_dict[msg] = records[0][0]

			message = '<table style="border-collapse:collapse" border="1"><thead>%s</thead></table>'
			xyz = ''
			for key, value in stats_dict.iteritems():
				x = "<tr><td>%s</td><td>%s</td></td>" %(key, value)
				xyz += x

			message = message % xyz
			self.sendmail("bnsagar90@gmail.com", "niranjan@headrun.com", message)
		except:
			print traceback.format_exc()

	def run_main(self):
		self.collect_stats()



if __name__ == "__main__":
	obj = MonitoringScript()
	obj.run_main()
