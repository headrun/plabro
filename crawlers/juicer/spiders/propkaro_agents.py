from juicer.utils import *
from dateutil import parser
import MySQLdb
from scrapy.http import *

con=MySQLdb.connect(db="AGENTS",user="root",charset="utf8",use_unicode=True)
cur = con.cursor()

class Propkaro(JuicerSpider):
	name='propkaro'
	cities = [ 'Gurgaon', 'Delhi', 'Greater Noida', 'Noida', 'Faridabad', 'Ghaziabad']

	def start_requests(self):
		url = "http://propkaro.com/jquery_post.php"
		formdata  = {
					 'is_location_search' : '0',
					}

		for city in self.cities:
			formdata['city'] = "\'%s\'" %(city)
			yield FormRequest(url, callback = self.parse, formdata = formdata)

	def parse(self,response):
		print response.url
		hdoc=HTML(response)
		agent_links  = hdoc.select('//a[@style="font-weight:600 !important; font-size:13px;"]/@href').extract()
		for agent_link in agent_links:
			yield Request(agent_link,callback=self.profile,priority=0)
	
	def profile(self,response):
		hdoc=HTML(response)
		location     = textify(hdoc.select('//tr[th[contains(text(),"Detailed location:")]]/td/text()').extract())
		profile_link  = textify(hdoc.select('//p/a[contains(text(),"View Profile")]/@href').extract())

		yield Request(profile_link,callback=self.details,priority=1,meta={"location":location})

	def details(self,response):
		hdoc=HTML(response)
		agency_name  = textify(hdoc.select('//div[@class="prfl-top-r"]/h1/text()'))
		email_id     = textify(hdoc.select('//div[@class="prfl-top-r"]/p[1]/text()'))
		phone_number        = textify(hdoc.select('//div[@class="prfl-top-r"]/p[2]/text()'))
		reference_url       = response.url
		sk                  = reference_url.split('/')[-1]
		address     = response.meta["location"]
		a    = reference_url.split('/')[-3]
		city=a.split('-')[-1]
		print '*'*20
		print agency_name
		print email_id
		print phone_number
		print reference_url
		print sk
		print address
		print city
		query = "insert into propkaro_agents( sk, agency_name, city, email_id, phone_number, address, reference_url, modified_at, created_at)values('%s', '%s', '%s', '%s', '%s', '%s', '%s', now(), now()) on duplicate key  update modified_at=now()"
		values = (sk,agency_name,city,email_id, phone_number, address,reference_url)

		cur.execute(query % values)

