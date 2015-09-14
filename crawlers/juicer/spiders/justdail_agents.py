import json
import MySQLdb
from juicer.utils import *

con = MySQLdb.connect(host='localhost', user= 'root',db="AGENTS",charset="utf8",use_unicode=True)
cur = con.cursor()

links = ['http://www.justdial.com/%s/Estate-Agents',
		 'http://www.justdial.com/%s/Estate-Agents-For-Residential-Rental/',
		 'http://www.justdial.com/%s/Estate-Agents-For-Residence/',
		 'http://www.justdial.com/%s/Estate-Agents-For-Rental-Commercial-Spaces/',
		 'http://www.justdial.com/%s/Estate-Agents-For-Land/',
		 'http://www.justdial.com/%s/Estate-Agents-For-Plot/',
		 'http://www.justdial.com/%s/Estate-Agents-for-Commercial-Spaces/',
		 'http://www.justdial.com/%s/Estate-Agents-For-Rental/',
		 'http://www.justdial.com/%s/Estate-Agents-For-Commercial-Space-On-Lease/',
		 'http://www.justdial.com/%s/Estate-Agents-For-ATM-Centre/',
		 'http://www.justdial.com/%s/Estate-Agents-For-Bungalow/',
		 'http://www.justdial.com/%s/Estate-Agents-For-Plot-Seller/',
		 'http://www.justdial.com/%s/Estate-Agents-For-Agricultural-Land/',
		 'http://www.justdial.com/%s/Estate-Agents-For-Paying-Guest/',
		 'http://www.justdial.com/%s/Estate-Agents-For-Residential-Rental/']

class JUSTDIAL(JuicerSpider):
		name        = 'justdail_agent'
		start_urls  =  ['http://www.justdial.com/autosuggest.php?cases=popCity&search=%40A%40&scity=']

		def __init__(self, *args, **kwargs):
			super(JUSTDIAL).__init__(self, *args, **kwargs)
			self.crawl_type = self.kwargs.get('crawl_type', "keepup")
			
		def parse(self,response):
			temp 	= json.loads(response.body)
			cities = ['gurgoan', 'ghaziabad', 'noida', 'faridabad']

			for i in temp['results']:
				for link in links:
					link 	= link %i["value"]
					city    = i["value"]
					city 	= city.capitalize()

					yield Request(link, callback=self.parse_cities,
								    dont_filter = True, priority=0,
									meta = {'org_link' : link,
											'page' : 1,
											'city' : city })

		def parse_cities(self,response):
			sel			 = HTML(response)
			city		 = response.meta['city']

			print city
			agents_nodes = sel.select('//section[@class="jbbg"]//section[@class="jrcl "]//aside[@class="compdt"]//span[@class="jcn "]//a/@href')
			if not agents_nodes:
				agents_nodes = sel.select('//section[@class="jgbg"]//section[@class="jrcl "]//aside[@class="compdt"]//span[contains(@class,"jcn")]//a/@href')

			for agent in agents_nodes:
				link = textify(agent)
				yield Request(link, callback = self.parse_agent, meta = {'city' : response.meta['city']}, priority=1)

			if self.crawl_type == "keepup":	
				for i in range(1,50):
					link = "%s/page-%s" %(response.meta['org_link'], i)
					yield Request(link, callback = self.parse_cities, meta = {'org_link' : response.meta['org_link'],
																			  'page' : i,
																			  'city' : response.meta['city'] },
																			   priority = 1)
			
		def parse_agent(self, response):
				sel = HTML(response)
				title 	=  textify(sel.select('//section[@class="jbbg jddtl"]//section[@class="jcar_wrp"]//h1//span[@title]/@title'))
				ph_nume = textify(sel.select('//section[@class="jbbg jddtl"]//a[@class="tel"]//text()'))
				sk = response.url.split("/")[4]
				print "%s <> %s <> %s <> %s" %(title, ph_nume, sk, response.meta['city'])
				query = 'insert into justdial_agents_full(sk, agency_name, city, phone_number, address, reference_url, created_at, modified_at) values ( %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
				values = (sk, title, response.meta['city'], ph_nume, '', response.url)
				cur.execute(query, values)
