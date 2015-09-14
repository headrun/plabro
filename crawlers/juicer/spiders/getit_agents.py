from juicer.utils import *

cursor = MySQLdb.connect(db="AGENTS", user="root", host="localhost").cursor()

class Getit(JuicerSpider):
	name = "getit_agents"
	cities = ['Mumbai', 'Delhi', 'Bangalore', 'Kolkata', 'Chennai', 
			  'Hyderabad', 'Jaipur', 'Ahmedabad', 'Pune', 'Surat',
			  'Nagpur', 'Indore', 'Thane', 'Ludhiana', 'Agra']
	start_urls = ['http://www.getit.in/%s/real-estate-agents-dealers/all-area/' %city.lower() for city in cities]

	def __init_(self, *args, **kwargs):
		super(Getit, self).__init__(*args, **kwargs) 
		self.crawl_type = kwargs.get('crawl_type', 'keepup')

	def parse(self, response):
		print response.url
		hdoc = HTML(response)
		agent_links = hdoc.select('//a[@class="srchtitlcls"]/@href')

		city = response.url.split("/")[3]

		for agent in agent_links:
			link = textify(agent)
			yield Request(link, callback = self.parse_details, priority =1, meta = {'city' : city.capitalize() })

		if self.crawl_type == "catchup":
			number_of_pages = ''.join(hdoc.select('//td[@valign="middle"]//b//text()').extract()[2])
			for i in range(1, int(number_of_pages)):
				link = "http://www.getit.in/%s/real-estate-agents-dealers/all-area/similar-results/%s/" %(city, i)
				yield Request(link, callback = self.parse, priority=1)

	def parse_details(self, response):
		hdoc = HTML(response)

		agent_name 	= textify(hdoc.select('//li[@class="area"]//div//span/h1/text()'))	
		address 	= textify(hdoc.select('//span[@class="gt_detl_address_color"]//text()'))
		mobile_number =  textify(hdoc.select('//span[@class="gt_detl_mob_color"]//text()'))
		landline_number = textify(hdoc.select('//span[@class="gt_detl_tele_color"]//text()'))

		phone_number = ''
		if mobile_number and landline_number:
			phone_number = "%s<>%s" %(mobile_number, landline_number)
		elif mobile_number:
			phone_number = mobile_number
		elif landline_number:
			phone_number = landline_number

		query = 'insert into getit_agents(sk, agency_name, city, phone_number, address, reference_url,\
                 created_at, modified_at) values(%s, %s, %s, %s, %s, %s, now(), now()) on \
                 duplicate key update phone_number=%s'
		values = (response.url.split("/")[-1].replace(".html", '').strip(),
					agent_name, response.meta['city'], phone_number, address, response.url, phone_number)
		cursor.execute(query, values)
