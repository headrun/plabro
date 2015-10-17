from juicer.utils import *
from pprint import pprint
import json
import re
import MySQLdb
import time
import datetime
from scrapy.http import Request
from scrapy.spider import BaseSpider

class Proptig(JuicerSpider):
		name 		= 'proptiger_crawl'
		start_urls 	= []

		def start_requests(self):
				self.location_dict = {
								 '20' : 'Noida', 		'2'   : "Bangalore",
								 '18' : "Mumbai", 		'21'  : "Pune",
								 '11' : "Gurgaon", 		"1"   : "Ahmedabad",
								 '5'  : "Chennai", 		"16"  : "Kolkata",
								 '6'  : "Delhi", 		'8'   : "Ghaziabad",
								 "41" : "Visakhapatnam", "12" : "Hyderabad",
								 "88" : "Faridabad", 	"25"  : "Nagpur",
								 "26" : "Coimbatore", 	"29"  : "Trivandrum",
								 "31" : "Bhubaneswar", 	"36"  : "Mysore",
								 "39" : "Mangalore", 	'24'  : 'Chandigarh',
								 "28" : "Jaipur", 		"102" : "Ludhiana",
								 "40" : "Surat", 		"13"  : "Indore"}
								
				self.location_dict_reverse = {   'ghaziabad'  	: '1',}


				#for locid in self.location_dict_reverse.keys():
				for locid, location in self.location_dict.iteritems():
					link = ['https://www.proptiger.com/app/v2/project-listing?selector={"filters":{"and":[{"equal":{"cityId":%s}},{"equal":{"cityId":%s}}]},"paging":{"start":1,"rows":10000}}',
					'https://www.proptiger.com/app/v2/project-listing?selector={"filters":{"and":[{"equal":{"cityId":%s}},{"equal":{"cityId":%s}}]},"paging":{"start":0,"rows":10000}}',
					'https://www.proptiger.com/app/v2/project-listing?selector={"filters":{"and":[{"equal":{"cityId":%s}},{"equal":{"unitType":"Apartment"}},{"equal":{"isResale":["true"]}},{"equal":{"cityId":%s}},{"equal":{"unitType":"Apartment"}},{"equal":{"isResale":["true"]}}]},"paging":{"start":0,"rows":10000}}',
					'https://www.proptiger.com/app/v2/project-listing?selector={"filters":{"and":[{"equal":{"cityId":%s}},{"equal":{"projectStatus":["pre launch","not launched"]}},{"equal":{"cityId":%s}},{"equal":{"projectStatus":["pre launch","not launched"]}}]},"paging":{"start":0,"rows":10000}}',
					'https://www.proptiger.com/app/v2/project-listing?selector={"filters":{"and":[{"equal":{"cityId":%s}},{"equal":{"unitType":"Apartment"}},{"equal":{"projectStatus":["ready for possession","occupied","completed"]}},{"equal":{"cityId":%s}},{"equal":{"unitType":"Apartment"}},{"equal":{"projectStatus":["ready for possession","occupied","completed"]}}]},"paging":{"start":0,"rows":10000}}',
					'https://www.proptiger.com/app/v2/project-listing?selector={"filters":{"and":[{"equal":{"cityId":%s}},{"equal":{"unitType":"Apartment"}},{"equal":{"projectStatus":["ready for possession","occupied","completed"]}},{"equal":{"cityId":%s}},{"equal":{"unitType":"Apartment"}},{"equal":{"projectStatus":["ready for possession","occupied","completed"]}}]},"paging":{"start":0,"rows":10000}}',
					'https://www.proptiger.com/app/v2/project-listing?selector={"filters":{"and":[{"equal":{"cityId":%s}},{"equal":{"unitType":"Apartment"}},{"equal":{"cityId":%s}},{"equal":{"unitType":"Apartment"}}]},"paging":{"start":0,"rows":10000}}',
					'https://www.proptiger.com/app/v2/project-listing?selector={"filters":{"and":[{"equal":{"cityId":%s}},{"equal":{"unitType":"Villa"}},{"equal":{"cityId":%s}},{"equal":{"unitType":"Villa"}}]},"paging":{"start":0,"rows":10000}}',
					'https://www.proptiger.com/app/v2/project-listing?selector={"filters":{"and":[{"equal":{"cityId":%s}},{"equal":{"unitType":"Plot"}},{"equal":{"cityId":%s}},{"equal":{"unitType":"Plot"}}]},"paging":{"start":0,"rows":10000}}',
					'https://www.proptiger.com/app/v2/project-listing?selector={"filters":{"and":[{"equal":{"cityId":%s}},{"equal":{"unitType":"Apartment"}},{"equal":{"projectStatus":["launch"]}},{"equal":{"cityId":%s}},{"equal":{"unitType":"Apartment"}},{"equal":{"projectStatus":["launch"]}}]},"paging":{"start":0,"rows":10000}}'
					]
					for i in link:
						links = i %(locid, locid)
						yield Request(links, callback=self.parse, meta= {'city' : location}, priority= 1)

		def parse(self, response):
			temp_data = json.loads(response.body)
			temp_data = temp_data['data']['items']

			pagination = False
			for proj in temp_data:
				item			= Item(response)
				pagination = True
				item.set('builder_id', proj['builder']['id'])
				item.set('project_id', proj['projectId'])
				item.set('text', '')
				item.set('address', proj['address'])

				li_dict = response.url.split("=")[1].split("&")[0]
				li_dict = eval(urllib.unquote(li_dict))
				city = response.meta['city'].capitalize()
				item.set('city', city.strip())
				item.set('image', proj['imageURL'])
				item.set('lattitude', proj.get('latitude', ''))
				item.set('longitude', proj.get('longitude', ''))
				city_id 		= proj['locality']['cityId'] #need to check this

				item.set('title', proj['locality']['label'])
				if proj.get("URL", ""):
					if not "http" in proj.get("URL", ""):
						url = "https://www.proptiger.com/%s" %proj.get("URL", "")
						item.set('project_url', url)
				else:
					item.set('project_url', '')

				item.set('suburb', '')
				#proj['locality']['suburb']['label']
				if proj.has_key('locality'):
					if proj['locality'].has_key('suburb'):
						item.set('suburb', proj['locality']['suburb']['label'])
				else:
					item.set('suburb', '')

				item.set('possessionDate', proj.get('possessionDate', ''))
				if proj.get('possessionDate', ''):
					dt = int(proj.get('possessionDate', ''))/1000
					dt = datetime.datetime.fromtimestamp(dt)
					dt = str(dt)
					item.set('possessionDate', dt)

				item.set('average_per_unit_price', '')
				if proj.has_key('locality'):
					if proj['locality'].has_key('avgPricePerUnitArea'):
						item.set('average_per_unit_price', proj['locality']['avgPricePerUnitArea'])

				item.set('builder_name', '')
				item.set('builder_url', '')
				if proj.has_key('builder'):
					item.set('title', proj['builder'].get('name', ''))
					
					url = proj['builder'].get('url', '')
					if not "http" in proj['builder'].get('url'):
						url = "https://www.proptiger.com/%s" %proj['builder'].get('url')
						item.set('builder_url', url)

				properties = proj['properties']
				for prop in properties:
					temp = {}
					temp['property_id'] 		= prop['propertyId']
					temp['price_per_unit'] = prop.get('pricePerUnitArea', '')
					temp['no_of_bedrooms'] 			= prop.get('bedrooms', '')
					temp['no_of_bathrooms']			= prop.get('bathrooms', '')
					temp['prop_type']			= prop.get('unitType', '')
					temp['prop_type']			= "%s %s" %(prop.get('unitType', ''), prop.get('unitName', ''))
					temp['prop_size']			= prop.get('size', '')
					temp['prop_url']			= prop.get('URL', '')

					if not "http" in temp['prop_url']:
						url = "https://www.proptiger.com/%s" %temp['prop_url']
						temp['prop_url'] = url

					temp['is_sold_out']			= prop.get('isPropertySoldOut', "False")
					if temp['is_sold_out'] == 'false':
						temp['minprice'] 	  = prop.get('minResaleOrPrimaryPrice', '')
						temp['maxprice'] = prop.get('maxResaleOrPrimaryPrice', '')
					else:
						temp['minprice'] = temp['maxprice'] = ''

					temp['no_of_balconies']	 = prop.get('balcony', '')
					temp['no_of_study_rooms']  = prop.get('studyRoom', '')
					item.set('other_data', temp)
					title =  url.split("/")[-2].replace("-", " ").replace(str(prop['propertyId']), '').capitalize()
					temp['prop_name']           = title
					yield item.process()	
