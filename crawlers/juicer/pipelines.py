import os
import MySQLdb
import shutil
import datetime
import traceback
import time
import uuid
import tempfile
from hashlib import md5
try:
    import json
except:
    import simplejson as json

from scrapy.conf import settings
from utils import get_cursor, get_current_timestamp, get_datetime, xcode, ensure_json
#from cloudlibs import proxy

DEFAULT_ITEMS_TO_PROCESS = 100
DB_NAME = "REAL_ESTATES"
HOST = "localhost"

table1 = "insert into %s"
QUERY = "(sk, title, text, address, status, project_society, price, rent, posted_on, square_feet_price, area, landmark, type_of_ownership, configuration, contact_details, contact_number, url, modified_at, created_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), contact_number=%s"

QUERY_X = 'insert into proptiger (sk, builder_id, property_id, builder_name, project_name, city, lattitude, longitude, possessionDate, price_per_unit, address, no_of_balconies, no_of_bathrooms, no_of_bedrooms, no_of_study_rooms, is_sold_out, maxprice, minprice, average_price_per_unit, prop_size, prop_type, image, builder_url, prop_url, project_url, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now();'

class JuicerPipeline(object):
	def __init__(self):
		self.conn = MySQLdb.connect(db=DB_NAME, host=HOST, user="root", use_unicode=True)
		self.cursor = self.conn.cursor()
	
	def process_item(self, item, spider):
		try:
			spider_name = spider.name.split("_")[0]
			if spider_name != "proptiger":
					sk 		= item['sk']
					item 	= item['data']
					table 	= table1 % spider.name.split("_")[0]
					query 	= table + QUERY
					values 	= (	sk ,item['title'], item['text'], item['address'], item['status'],
								item['project_society'], item['price'], item['rent'], 
								datetime.datetime.fromtimestamp(item['posted_on']),
								item['square_feet_price'], json.dumps(item['area']),
								item['landmark'], item['type_of_ownership'],
								item['configuration'], item['contact_details'],
								item['contact_number'],
								item['url'], item['contact_number'])
					self.cursor.execute(query, values)
					self.conn.commit()
			else:
				sk      = md5(item['data']['other_data']['prop_url']).hexdigest()
				item    = item['data']
			
				values = (	sk, str(item['builder_id']), str(item['other_data']['property_id']),
							item['title'],
							item['other_data']['prop_name'], item['city'], str(item['lattitude']), str(item['longitude']),
							item['possessionDate'], str(item['other_data']['price_per_unit']), 
							item['address'], str(item['other_data']['no_of_balconies']),
							str(item['other_data']['no_of_bathrooms']),
							str(item['other_data']['no_of_bedrooms']),
							str(item['other_data']['no_of_study_rooms']),
							item['other_data']['is_sold_out'],
							str(item['other_data']['maxprice']),
							str(item['other_data']['minprice']), 
							str(item['average_per_unit_price']), str(item['other_data']['prop_size']),
							item['other_data']['prop_type'], item['image'], item['builder_url'],
							item['other_data']['prop_url'], item['project_url'])

				self.cursor.execute(QUERY_X, values)
				self.conn.commit()
		except:
			print traceback.format_exc()
	
		return item


	'''
	def close_spider(self, spider):
    	self._send_items()
	'''
