from juicer.utils import *
from dateutil import parser
import re
import json
import urllib
import MySQLdb

db_name = "REAL_ESTATES"

class Quikr(JuicerSpider):
		name = 'quikr_browse'
		url = "http://www.quikr.com/homes/adsdata?c=r&t=b&p=1&pt=ap,vi,rp,bf&r=&am=&f=&l=%s&ct=%s&ci=27&page=0"
		start_urls = []
		localities = {'Gurgoan' : ['DLF Phase 3', 'DLF Phase 5', 'Golf Course Road', 'Golf Course Road', 'Sohna Road'],
					  'Delhi' 	: ['Dwarka', 'Patel Nagar', 'Uttam Nagar', 'Malviya Nagar'],
					  'Mumbai' 	: ['Mira Road', 'Andheri East', 'Andheri West', 'Malad West'],
					  'Pune' 	: ['Hadapsar', 'Wakad', 'Kharadi', 'Kothrud'],
				      'Bangalore' : ['Koramangala', 'H.S.R. Layout', 'Whitefield', 'Electronic City'],
					 }

		for city, locales in localities.iteritems():
			for local in locales:
				city = urllib.quote(city.lower())
				link = url %(local, city.lower())
				start_urls.append(link)

		def __init__(self, *args, **kwargs):
			super(Quikr, self).__init__(*args, **kwargs)
			
			self.cursor = MySQLdb.connect(db=db_name, host="localhost", user="root")
			self.cursor = self.cursor.cursor()


		def parse(self,response):
			json_data = json.loads(response.body)

			city = [ i.split("=")[-1] for i in response.url.split("&") if "ct" in i]
			for jd in json_data['data']:
				list_id 	=  jd['id']
				lat			=  jd['lat']
				long1		=  jd['long1']
				bhk			=  jd['bhk']
				price		=  jd['price']
				location 	=  jd['location']
				sqft		=  jd['sqft']
				furnished	=  jd['furnished']
				postedBy	=  jd['postedBy']
				amenities	=  ','.join(jd['amenities'])
				date		=  jd['date']
				propertyType	= jd['propertyType']
				propertyTypeAvailable	= jd['propertyTypeAvailable']
				availableFor	= jd['availableFor']
				adTitle			= jd['adTitle']
				isPremium		= jd.get('isPremium', '')
				profile			= jd['profile']
				city = ''.join(city)
				dealer_name = ''
				email_id = ''
				if profile:
					if profile.has_key('name'):
						if "@" in dealer_name:
							email_id =  profile['name']
						elif profile.has_key('name') == "null":
							dealer_name =  profile['name']

				if isinstance(location, list):
					location = ','.join(location)
				
				link = "http://www.quikr.com/homes/listing/%s" %list_id
				query = "insert into quikr_properties(sk, title, city, locality, latitude, longitude, bhk, sqft, furnished, postedBy, amenities, date, propertyType, propertyTypeAvailable, availableFor, isPremium, dealer_name, email_id, reference_url, created_at, modified_at)  values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
				values = (list_id, adTitle, city, location, lat, long1, bhk, sqft, furnished, postedBy, amenities, date, propertyType, propertyTypeAvailable, availableFor, isPremium, dealer_name, email_id, link)
				print query % values		

				self.cursor.execute(query, values)	
				yield Request(link, callback = self.home_listing, priority = 1)
				

		def home_listing(self, response):
			hdoc = HTML(response)

			script_data = hdoc.select('//script//text()').extract()
			sk = response.url.split("/")[-1]
			for scrp in script_data:
				if "viewAdRespObj" in scrp:
					data = re.findall("viewAdRespObj = JSON.parse\(('.*)\);" , scrp)
					data = data[0].replace("\\", "")
					for d in data.split(","):
						if "mobileNo" in d:
							phone_number = d.split(":")[-1].replace('"', '')
							query = 'update quikr_properties set contact_number = %s where sk = %s limit 1' %(phone_number, sk)
							self.cursor.execute(query)
