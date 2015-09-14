from juicer.utils import *
from dateutil import parser
from scrapy.http import FormRequest, Request

import MySQLdb
con=MySQLdb.connect(db="REAL_ESTATES",user="root",charset="utf8",use_unicode=True)
cur=con.cursor()


class IndiaMart(JuicerSpider):
	name = "indiamart"
	start_urls = ['http://dir.indiamart.com/impcat/real-estate-agent.html']

	def __init__(self, *args, **kwargs):
		super(IndiaMart).__init__(self, *args, **kwargs)
		self.crawl_type = kwargs.get("crawl_type", "keepup")

	def start_requests(self):
		url = "http://dir.indiamart.com/impcatProductPagination.php"
		
		formdata = {	'mcatName' 	: "Real Estate Agent",
						'mcatId' 	: "85989",
						'catId' 	: "323",
						'searchCity' : "0",
						"end" 	: "28",
						"rand" 	: "4",
						"prod_serv" : "S",
						"showkm" 	: "0",
						"debug_mod" : "0",
						"biz" : ''
					}

		yield FormRequest(url, 	callback = self.parse, 
							   	formdata = formdata,
								meta = {'formdata' : formdata})

	def parse(self, response):
		hdoc = HTML(response)
		agents = hdoc.select('//li[@class="listing-wdt"]')
		for agent in agents:
			agency1 = textify(agent.select('.//div[@class="listing-description-flexible"]/a/text()'))
			agency  = agency1.replace("&amp;","&")
			agency_name1 = textify(agent.select('.//div[@class="listing-address-container"]/p/span/span/text()'))
			agency_name  = agency_name1.replace("&amp;","&")
			phone = textify(agent.select('.//span[@itemprop="telephone"]/text()'))
			city  = textify(agent.select('.//span[@class="cityLocation-grid"]/text()'))
			address = textify(agent.select('.//span[@class="srch-add"]//span/text()'))
			website = textify(agent.select('.//span[@itemprop="url"]/a/@href'))
			#description = textify(agent.select('.//div[@class="listing-description-flexible"]/p/text()'))
			
			insert = "insert into indiamart_agents  values(%s, %s, %s, %s, %s, %s)"
			values=(agency,agency_name,phone,city,address,website)
			cur.execute(insert,values)
			con.commit()


		if self.crawl_type == "catchup":	
				# This is for the purpose of pagination	
				formdata = response.meta['formdata']
				formdata["end"] = int(formdata['end']) +  20
				formdata["end"] = str(formdata["end"])
				yield FormRequest(response.url, callback = self.parse, 
												formdata = formdata,
												meta = {'formdata' : formdata})

		
