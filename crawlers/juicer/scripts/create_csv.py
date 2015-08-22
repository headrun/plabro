import os
import MySQLdb
import datetime
import traceback
from datetime import timedelta
from utils import *

class CreateCSV(Utils):
	def __init__(self):
		Utils.__init__(self)
		self.db_name 	= "REAL_ESTATES"
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

			self.file_name  = "%s_%s.csv" %(source, self.date)
			self.op_file    = open(self.file_name, "w+")
			header = ["title", "text", "address", "status", "project_society", "price", 
					  "rent", "posted_on", "square_feet_price", "area", "landmark", 
					  "type_of_ownership", "configuration", "contact_details",
					  "contact_number", "url"]

			header = ",".join(header)
			self.op_file.write("%s \n" %header)
	
			one_day_gap = datetime.datetime.now() - timedelta(hours=24)
			query = "select * from %s where created_at > '%s'" %(source, str(one_day_gap))
			self.cursor.execute(query)
			records = self.cursor.fetchall()

			self.logger.info("Number of records for %s - %s" %(source, len(records)))
			for record in records:
				id, sk, title, text, address, status, project_society, price,\
					 rent, posted_on, square_feet_price, area, landmark, type_of_ownership,\
						 configuration, contact_details, \
							contact_number, url, created_at, modified_at = record

				if not contact_number:
					contact_number = ''

				data =  [self.normalize(title), self.normalize(text),
						 self.normalize(address),
						 status, self.normalize(project_society), 
						 price, rent, str(posted_on), square_feet_price, area, 
						 landmark, type_of_ownership, configuration, 
						 self.normalize(contact_details), contact_number, url]	
				
				self.op_file.write("%s\n" %(",".join(data)))	
			self.move_files(source)
		except:
			print traceback.format_exc()
			self.logger.info(traceback.format_exc())

	def move_files(self, source):
		try:
			cmd = "scp %s headrun@dbg.plabro.com:/home/headrun/%s" %(self.file_name, source)
			status = os.popen(cmd)
			self.logger.info("".join(status.readlines()))
		except:
			self.logger.info(traceback.format_exc())

	def run_main(self):
		self.create_cursor()
		for source in ['magicbricks', 'quikr']:
			self.write_data_to_file(source)
		self.cursor.close()

if __name__ == "__main__":
	CCsvObj = CreateCSV()
	CCsvObj.run_main()
	
