
from juicer.utils import *
from dateutil import parser

import MySQLdb
con=MySQLdb.connect(db="AGENTS",user="root",charset="utf8",use_unicode=True)
cur=con.cursor()

class commonfloor(JuicerSpider):
		name = 'commonfloor_agents'
		start_urls =['https://www.commonfloor.com/real-estate-agents-india']

		def parse(self,response):
			hdoc 	= HTML(response)
			cities 	= hdoc.select('//td/a/text()').extract()
			link	= 'https://www.commonfloor.com/realestate-agent/search-agent?city=%s'
			l ='&locality_scope=Search%20Location&community=Search%20Agent%2FCompany%20Name&locality_scope_id=&agent_name_id=&deal_type=&property_type=&sort_by=relevance&page=1&r=a&page_size=20000'
			for city in cities:
				links = link%(str(city))+l
				yield Request(links,callback=self.agents, priority=1)

		def agents(self,response):
			hdoc	= HTML(response)
			url		= response.url
			a		= url.split('?')
			b		= a[-1]
			c		= b.split('&')
			d		= c[0]
			e       = d.split('=')
			city	= e[-1]

			print city
			print "*"*30
			agent_links = hdoc.select('//div[@class="agent-list-item"]')
			for agent_link in agent_links:
				operating_areas = ''.join(agent_link.select('.//div[@class="agent-listitem-desc"]//div[@class="table-content-text"]//text()').extract())
				agents_link = ''.join(agent_link.select('.//div[@class="list-item-header"]//a/@href').extract())
				operating_areas = ''.join(agent_link.select('.//div[@class="agent-listitem-desc"]//div[@class="table-content-text"]/span[1]//text()').extract())
				print operating_areas
				yield Request(agents_link,callback=self.details,priority=1, meta = {'city':city, 'operating' : operating_areas})

		def details(self,response):
			hdoc = HTML(response)
			print response.url
			url	 = response.url
			city = response.meta['city']
			name = textify(hdoc.select('//h1[@class="font32Regular"]/text()')) or textify(hdoc.select('//h3[@class="name"]/text()'))
			agency = textify(hdoc.select('//div[@class="font14Regular"]/text()')) or textify(hdoc.select('//p[@class="profile"]/text()'))
			localities = textify(hdoc.select('//div[@id="localities_served"]/text()')) or textify(hdoc.select('//p[@class="address"]/text()'))  
			phone = textify(hdoc.select('//div[@id="vn"]/text()'))
			sale_listings=textify(hdoc.select('//div[span[contains(text(),"Sale :")]]/a[1]/span/text()')) or  textify(hdoc.select('//div[@class="listing-unit"]/span/text()'))

			rent_listings=textify(hdoc.select('//div[@class="font14Light details"]/span[contains(text(),"Rent :")]//following-sibling::a/span/text()')) or textify(hdoc.select('//div[@class="listing-unit listing-unit-right"]/span/text()'))

			sk = response.url.split("/")[-2]
			address = ''
			insert = "insert into commonfloor_agents(sk, agency_name, agent_name, city, phone_number, address, operating_areas, reference_url, created_at, modified_at) values(%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now(), operating_areas=%s"
			operating_areas = [ i.strip() for i in response.meta['operating'].split(',') ]
			operating_areas = '<>'.join(operating_areas)
			values = (sk, agency, name, city, phone, address, operating_areas, response.url, operating_areas)
			cur.execute(insert,values)
			con.commit()
