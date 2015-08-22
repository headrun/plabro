# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field
from scrapy.conf import settings

class JuicerItem(Item):
    	sk = Field()
    	spider = Field(default='')
    	update_mode = Field(default=settings.get('DEFAULT_UPDATE_MODE', 'overwrite'))
    	data = Field()

class Agents(Item):
		sk            = Field() 
		agency_name   = Field()
		agent_name    = Field()
		city          = Field()
		phone_number  = Field()
		address       = Field()
		aux_data      = Field()
		reference_url = Field()
		created_at    = Field()
		modified_at   = Field()
