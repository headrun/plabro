import urllib

BOT_NAME = 'juicer1'
BOT_VERSION = '1.0'

SPIDER_MODULES = ['juicer.spiders']
NEWSPIDER_MODULE = 'juicer.spiders'
DEFAULT_ITEM_CLASS = 'juicer.items.JuicerItem'

USER_AGENT = 'Mozilla/5.0 (X11; U; Linux i686; pl-PL; rv:1.9.0.2) Gecko/20121223 Ubuntu/9.25 (jaunty) Firefox/3.8'
USER_AGENT_LIST = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36'

ITEM_PIPELINES = [
	'juicer.pipelines.JuicerPipeline',
	# 'juicer.pipelines.MySQLdbPipeline'
]

#STATS_CLASS = 'juicer.utils.JuicerStatsCollector'

DB_NAME = "juicertest"
DB_HOST = "http://api.cloudlibs.com/db/"
DB_APP_ID = "4e8d4f84df3cb5386a000005"
DB_PIPE_ID = "b9BeD8qTRQlijr8FjXARk8gmmfv14vXI6BXSfRFy"
DB_PIPE_SECRET = "tGLKh0OF6uZQuV50TG2F550tJqOR4h5QCbhzAHzS"

HULK_MASTER_DB_NAME = "juicer"
HULK_MASTER = "http://localhost:9990"
HULK_MASTER_APP_ID = "1"

URLQ_TABLE = "url_queue"

HTTPCACHE_ENABLED = False
HTTPCACHE_DIR = '/home/dev/juicer/cache/'
HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_STORAGE = 'juicer.cache.LevelDBCacheStorage'

# COMMANDS_MODULE = 'juicer.commands'
DOWNLOAD_DELAY = 50
RANDOMIZE_DOWNLOAD_DELAY = True

LOG_FILE = None
LOG_LEVEL = 'INFO' #'DEBUG'

SCRIPT_LOG_FILE = 'juicer.log'

TELNETCONSOLE_ENABLED = False
WEBSERVICE_ENABLED = False

NUM_ITEMS_TO_CONSUME = 10000

CONCURRENT_SPIDERS = 8 	# Change: i have reduced it

SPIDER_MIDDLEWARES = {
    'juicer.utils.SpiderMiddleware': 10000,
}

DOWNLOADER_MIDDLEWARES = {
    'juicer.utils.RandomUserAgentMiddleware': 400,
    'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware': None,
    'juicer.randomproxy.RandomProxy': 100,
    #'scrapy.contrib.downloadermiddleware.httpproxy.HttpProxyMiddleware': 110,
}

DEFAULT_REQUEST_HEADERS = {
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Language': 'en',
'Accept-Encoding': '*'
}

RANDOM_SCHEDULING = True
DUMPSTORE_DIR = "/home/juicer2/dumpstore"
DEFAULT_CRAWLER_PRIORITY = 5
MIN_URLS_TO_GET = 10
URLQUEUES_DIR = '/home/dev/juicer/urlqueues/'

NO_ITEMS_TO_PROCESS = 100
NO_URLS_TO_PROCESS = 10000
NO_DUMPSTORE_ITEMS_TO_PROCESS = 10000

#PROXY_HOST = "localhost"
#PROXY_PORT = 8080

COUNTER_SERVICE_HOST = "http://api.cloudlibs.com/counter/"
COUNTER_SERVICE_APPID = "4f448734934c2b3139000000"

SEARCH_HOST = "http://api.cloudlibs.com/search/"
SEARCH_APP_ID = "4f27dd5e934c2b1c48000000"
SEARCH_PIPE_ID = "ejRUsdFSiFupDllRyuxdbCIMDOBIeRBbNwM37GOq"
SEARCH_PIPE_SECRET = "5D04G1zXEWf3GksrDNPce98HInup1NGyRVP7dXce"

USER_AGENT_LIST = ['Mozilla/5.0 (Macintosh; I; Intel Mac OS X 11_7_9; de-LI; rv:1.9b4) Gecko/2012010317 Firefox/10.0a4',
                    'Mozilla/5.0 (Windows NT 6.2; rv:9.0.1) Gecko/20100101 Firefox/9.0.1',
                    'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:5.0) Gecko/20100101 Firefox/5.0',
                    'Mozilla/5.0 (U; Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0',
                    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b8pre) Gecko/20101128 Firefox/4.0b8pre',
                    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0b7) Gecko/20100101 Firefox/4.0b7',
                    'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52',
                    'Opera/9.80 (X11; Linux x86_64; U; fr) Presto/2.9.168 Version/11.50',
                    'Opera/9.80 (X11; Linux i686; U; ja) Presto/2.7.62 Version/11.01',
                    'Mozilla/4.0 (compatible; MSIE 8.0; X11; Linux x86_64; pl) Opera 11.00',
                    'Opera/9.80 (Windows NT 5.1; U; pl) Presto/2.6.30 Version/10.62',
                    'Opera/9.80 (X11; Linux x86_64; U; it) Presto/2.2.15 Version/10.10',
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20',
                    'Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.65 Safari/535.11',
                    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Chrome/17.0.940.0 Safari/535.8',
                    'Mozilla/5.0 (X11; CrOS i686 1193.158.0) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.75 Safari/535.7',
                    'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.8 (KHTML, like Gecko) Chrome/16.0.912.63 Safari/535.8',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.874.54 Safari/535.2',
                    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; de-at) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1',
                    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; zh-cn) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
                    'Mozilla/5.0 (Windows; U; Windows NT 6.1; de-DE) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4',
                    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; th-th) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8',
                    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; ko-kr) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.16',
                    'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_6; en-gb) AppleWebKit/528.10+ (KHTML, like Gecko) Version/4.0dp1 Safari/526.11.2']


PROXY_LIST = '/home/dev/juicer/trunk/juicer/proxy.txt'
# PROXY_LIST = '/home/prod/juicer/trunk/juicer/list.txt'
# PROXIES_LIST = [ 'http://%s:8080' % ip.strip() for ip in file("/home/dev/juicer/trunk/juicer/spiders/LIST_OF_IPS", "r+").readlines()]
# PROXY_LIST = ['http://%s:3279' % ip.strip() for ip in urllib.urlopen('http://hosting.cloudlibs.com/static/misc/juicer_proxy_ips')]
