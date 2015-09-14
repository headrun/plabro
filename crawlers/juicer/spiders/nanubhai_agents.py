
from juicer.utils import *
from dateutil import parser
from scrapy.http import FormRequest, Request


class nanubhai(JuicerSpider):
	name		= "nanubhai"
	start_urls 	= ['http://www.nanubhaiproperty.com/login.aspx']

	def get_formdata(self, hdoc):
	
		formdata = {
					'__EVENTTARGET' 	: textify(hdoc.select('//input[@name="__EVENTTARGET"]/@value')),
					'__EVENTARGUMENT' 	: textify(hdoc.select('//input[@name="__EVENTARGUMENT"]/@value')),
					'__VIEWSTATE' 		: textify(hdoc.select('//input[@name="__VIEWSTATE"]/@value')),
					'ctrlToolkitScriptManager_HiddenField' : textify(hdoc.select('//input[@name="ctrlToolkitScriptManager_HiddenField"]/@value'))
					}

		return formdata
	
	def parse(self, response):
		hdoc = HTML(response)
		form_data = self.get_formdata(hdoc)	
	
		formdata = {
					'__EVENTTARGET' : form_data['__EVENTTARGET'],
					'__EVENTARGUMENT' : form_data['__EVENTARGUMENT'],
					'__VIEWSTATE' : form_data['__VIEWSTATE'],
					'ctrlToolkitScriptManager_HiddenField' : form_data['ctrlToolkitScriptManager_HiddenField'],
					"ctl00$ctl00$cph1$cph1$ctrlMemberLogin$LoginForm$UserName":"jaideep@headrun.com",
					"ctl00$ctl00$cph1$cph1$ctrlMemberLogin$LoginForm$Password":"27382063"}
		
		yield FormRequest.from_response(response, callback = self.parse_next, formdata = formdata)

	
	def parse_next(self,response):
		hdoc=HTML(response)
		url='http://www.nanubhaiproperty.com/view-real-estate-property-agents/agents-operating-and-based-in-bangalore.aspx?pageindex=84'
		''''for i in range(1,85):
			urls = "http://www.nanubhaiproperty.com/view-real-estate-property-agents/agents-operating-and-based-in-bangalore.aspx?pageindex="+str(i)
			print urls
		for url in urls:'''
		yield Request(url, callback = self.parse_details)

	def parse_details(self, response):
		hdoc = HTML(response)		
		agent_nodes=hdoc.select('//div[@class="panel panel-info margin-bottom-20"]')
		for agent in agent_nodes:
	 		agency = textify(agent.select('.//h3[@class="panel-title"]//text()'))
			agent_name = textify(agent.select('.//div[@class="col-sm-6 col-md-6"]/p/a/b/text()'))
			operiiating_localities  = textify(agent.select('.//p[b[contains (text(),"Operating In: ")]]/text()'))	
			contact_link=textify(hdoc.select('.//a[@id="cph1_ctrlAgentSearchResult_ctrlMain_rptAgentSearch_ctrlContactTabs_0_hplViewContactDetails_0"]/@href'))

			'''	
			print "*"*20
			print agency
			print agent_name
			print operating_localities
			print contact_link
			'''
			yield Request(contact_link,callback=self.contact)
	
	def contact(self,response):
			hdoc=HTML(response)
			'''
			curl 'http://www.nanubhaiproperty.com/contact-details-company/broker-nithesh-in-bangalore.aspx?cid=IMsegXkw90g=' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: en-US,en;q=0.8' -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36' -H 'Chrome-Proxy: ps=1440476929-2709066769-3547857805-1193759041, sid=63459d005e0c9b9653c7354246037567, c=linux, b=2311, p=135' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Referer: http://www.nanubhaiproperty.com/view-real-estate-property-agents/agents-operating-and-based-in-bangalore.aspx' -H 'Cookie: .ASPXANONYMOUS=gXwXz4wV0QEkAAAAOWE2ZTY3NjItMTk3Yy00NjM2LTg0ODUtMGNjMzY0MTA1NTdkN4Up66t7J7xnLQGh3vsn4Tzd5G41; ASP.NET_SessionId=igxzyzsgcdkgfk5uu1vgbtud; NB.AUTH=FF9967376FD206950BED964B1B77560C22A462D970571672DC7DD0AF5A51793E7772D023C953F56C1DA7B6455E0F9C6E47516DCA29CE7B221AE4A1011D8357089C7E87803452B73162CDBF165DCFDE2931A87343D8A0DF69A86CED5A917AFDD411691A8AF4C0B1235A79B70F05994F9D4C7342F99E95DE4EE36B3C57FB29874ABDB34500; TS2siih890c0_aPVGCqzU8u7aWlAzMCuo6hVNETfCv0-=20h03HA8ZPUU9s35TyArB1eN5GmezJ8s2sfcyNikmkNle5kVpxK13w--; __utma=97244416.2101158242.1440482285.1440482285.1440482285.1; __utmc=97244416; __utmz=97244416.1440482285.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)' -H 'Proxy-Connection: keep-alive' --compressed
			'''
			import pdb;pdb.set_trace()
			phone=textify(hdoc.select('//p[b[contains(text(),"Mobile:")]]/text()'))
			print phone
