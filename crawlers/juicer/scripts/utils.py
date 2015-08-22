import os
import sys
import smtplib
import MySQLdb
import traceback
import logging
import datetime

class Utils:
	def __init__(self):
		self.pwd = os.getcwd()
		self.timestamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
		self.logger()

	def open_cursor(self, db_ip, db_name):
		try:
			self.conn = MySQLdb.connect(db	 = db_name, 
										host = db_ip,
										user = "root")
			self.cursor = self.conn.cursor()
		except:
			print traceback.format_exc()

	def logger(self):
		try:
			process_name = sys.argv[0].split(".")[0]
			filename 	 = "%s_%s.log" %(process_name, self.timestamp)
			self.logger  = logging.getLogger(process_name)
			log_file 	 = os.path.join(self.pwd, filename)
			hdlr 		 = logging.FileHandler(log_file)
			formatter 	 = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
			hdlr.setFormatter(formatter)
			self.logger.addHandler(hdlr) 
			self.logger.setLevel(logging.INFO)	
		except:
			print traceback.format_exc()

	def sendmail(self, sender, receivers, message):
		try:
			smtpObj = smtplib.SMTP('localhost')
			smtpObj.sendmail(sender, receivers, message)
			print 'successfully sent the mail'
		except:
			print traceback.format_exc()
