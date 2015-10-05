import os
import re
import csv
import MySQLdb
import datetime
import traceback
from datetime import timedelta
from utils import *

class CreateCSV(Utils):
	def __init__(self):
		Utils.__init__(self)
		self.db_name 	= "AGENTS"
		self.db_ip 		= "localhost"
		self.db_user 	= "root"
		self.date		= datetime.datetime.now().strftime("%Y-%m-%d")

	def normalize(self, text):
		text  = text.replace("\t", " ").replace("\n", " ")
		return text

	def create_cursor(self):
		try:
			self.conn   = MySQLdb.connect(db=self.db_name, host=self.db_ip, user=self.db_user)
			self.cursor = self.conn.cursor()
		except:
			self.logger.info(traceback.format_exc())

	def write_data_to_file(self, source):
		try:
			self.logger.info("Processing ... %s for %s" %(source, self.date))
			self.data  = []
			self.file_name  = "%s_%s.csv" %(source, self.date)
			headers = ['agency_name', 'agent_name', 'city', 'phone', 'address']
			self.data.append(headers)

			query = 'select agency_name, agent_name, city, phone_number, address, reference_url from indiamart_agents where created_at > DATE_ADD(NOW(), INTERVAL -1 DAY)'

			self.cursor.execute(query)
			records = self.cursor.fetchall()

			if not records:
				message = 'Subject: %s\n\n%s' % ("%s %s" %(source, self.date), "No Records for today")
				self.sendmail('niranjan@headrun.com', ['plabro@headrun.com'], message)
				return
			else:
				message = 'Subject: %s\n\n%s' % ("%s %s" %(source, self.date), "Number of records : %s" %len(records))
				self.sendmail('niranjan@headrun.com', ['plabro@headrun.com'], message)

			self.logger.info("Number of records for %s - %s" %(source, len(records)))
			for record in records:
				agency, name, city, phone, address, reference_url = record
				if "," in phone:
					phone = "<>".join(phone.split(","))
				if re.findall('(.* \w+ - \d{6}),', address):
					address = re.findall('(.* \w+) - \d{6},', address)[0]
				data = [ agency, name, city, phone.replace(" ", ''), address.replace('(Map)', '').replace("...more", "").replace("...", '').replace("\r\r", "").replace("\t", "").strip()]
				self.data.append(data)
		

			fp = open(self.file_name, "w+")
			for d in self.data:
				fp.write("%s\n" %"_$$_".join(d))
			self.move_files(source)
		except:
			print traceback.format_exc()
			self.logger.info(traceback.format_exc())

	def move_files(self, source):
		try:
			cmd = "scp %s headrun@dbg.plabro.com:/home/headrun/%s" %(self.file_name, source.replace("_agents", ""))
			status = os.popen(cmd)
			self.logger.info("".join(status.readlines()))
		except:
			self.logger.info(traceback.format_exc())

	def run_main(self):
		self.create_cursor()
		for source in ['indiamart_agents']:
			self.write_data_to_file(source)
		self.cursor.close()

if __name__ == "__main__":
	CCsvObj = CreateCSV()
	CCsvObj.run_main()
	
