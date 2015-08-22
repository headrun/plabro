import os
import MySQLdb
import datetime
import calendar
import hashlib
import re
import copy
import time
import inspect
import urlparse
import traceback
import urllib
import random
import pprint
from itertools import chain
from urlparse import urljoin
from dateutil.parser import *
from dateutil.relativedelta import *
import sqlite3
from pkgutil import iter_modules
import socket
import string
import shutil
import redis
from lxml import html
from lxml.html.clean import clean_html

#NOTE: Some of the imports below may not be used here
# but are required because spiders import from here in this fashion
# from juicer.utils import *

from scrapy import log
from scrapy.conf import settings
from scrapy.statscol import MemoryStatsCollector
from scrapy.selector import HtmlXPathSelector
from scrapy.selector import XmlXPathSelector
from scrapy.spider import BaseSpider
from scrapy.http import Request as ScrapyHTTPRequest
from juicer.items import JuicerItem
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.utils.misc import walk_modules

import msgpack
import simplejson as json

URLQUEUES = {}
TWO_WEEKS = 1209600
ONE_HOUR = 3600


def _ensure_urlqueue(fpath):
    conn = sqlite3.connect(fpath)

    cursor = conn.cursor()
    cursor.execute('select name from sqlite_master where type = "table";')
    tables = [r[0] for r in cursor.fetchall()]

    if 'urlqueue' not in tables:
        cursor.execute('create table urlqueue (sk varchar(255) primary key, url varchar(4096), got_page integer, data text);')
        cursor.execute('create index got_page_idx on urlqueue (got_page);')

    conn.commit()
    cursor.close()

def _drop_urlqueue(spider_name):
    urlqueues_dir = settings['URLQUEUES_DIR']
    fpath = os.path.join(urlqueues_dir, spider_name)
    if os.path.exists(fpath):
        os.remove(fpath)

    if spider_name in URLQUEUES:
        del URLQUEUES[spider_name]

def _get_urlqueue(spider_name):
    if spider_name in URLQUEUES:
        return URLQUEUES[spider_name]

    urlqueues_dir = settings['URLQUEUES_DIR']
    if not os.path.exists(urlqueues_dir):
        os.makedirs(urlqueues_dir)

    fpath = os.path.join(urlqueues_dir, spider_name)
    _ensure_urlqueue(fpath)

    conn = sqlite3.connect(fpath)

    URLQUEUES[spider_name] = conn
    return _get_urlqueue(spider_name)

def _update_urlqueue(spider_name, url, sk, got_page, data=None):
    conn = _get_urlqueue(spider_name)
    cursor = conn.cursor()

    command = 'REPLACE' if got_page else 'IGNORE'
    data = repr(data)
    query = 'INSERT OR %s INTO urlqueue (sk, url, got_page, data) VALUES (?, ?, ?, ?)' % command
    cursor.execute(query, (sk, url, got_page, data))
    cursor.close()
    conn.commit()

def get_timestamp(dt):
    t = dt.utctimetuple()
    return calendar.timegm(t)

def get_datetime(epoch):
    t = time.gmtime(epoch)
    dt = datetime.datetime(*t[:6])
    return dt

def get_current_timestamp():
    return get_timestamp(datetime.datetime.utcnow())

def get_page(spider_name, url, sk=None, data=None):
    sk = sk or md5(url)
    _update_urlqueue(spider_name, url, sk, 0, data)

def get_cursor(db_host=None, db_pipe_id=None, db_pipe_secret=None, db_app_id=None):
    db = proxy(db_host or settings['DB_HOST'], db_app_id or settings['DB_APP_ID'])
    db._set("pipe", [db_pipe_id or settings['DB_PIPE_ID'], db_pipe_secret or settings['DB_PIPE_SECRET']])
    return db, settings['DB_NAME']

def got_page(spider_name, response=None, sk=None, url=None):
    assert(url or response)
    url = url or get_request_url(response)
    sk = sk or md5(url)
    _update_urlqueue(spider_name, url, sk, 1)

def make_var(x):
    return re.sub(' ', '_', re.sub(r'\s+', ' ', re.sub('[^a-z0-9_]', ' ', x.lower())))

def md5(x):
    return hashlib.md5(xcode(x)).hexdigest()

def parse_date(data, dayfirst=False):
    if not 'ago' in data and 'Yesterday' not in data:
        return parse(data, dayfirst=dayfirst, fuzzy=True)
    elif 'Yesterday' in data:
        return parse(data, dayfirst=dayfirst, fuzzy=True)+relativedelta(days=-1)
    else:
        DEFAULT = datetime.datetime.utcnow()
        dat = re.findall('\d+', data)
        if len(dat)==1 : dat.append(0)
        if 'years' in data:
            return DEFAULT + relativedelta(years=-int(dat[0]), months=-int(dat[1]))
        elif 'months' in data:
            return DEFAULT + relativedelta(months=-int(dat[0]), weeks=-int(dat[1]))
        elif 'week' in data:
            return DEFAULT + relativedelta(weeks=-int(dat[0]))
        elif 'day' in data:
            return DEFAULT + relativedelta(days=-int(dat[0]), hours=-int(dat[1]))
        elif 'hour' in data or 'hrs' in data or 'hr' in data:
            return DEFAULT + relativedelta(hours=-int(dat[0]), minutes=-int(dat[1]))
        elif 'minute' in data or 'mins' in data:
            return DEFAULT + relativedelta(minutes=-int(dat[0]))
        elif 'second' in data:
            return DEFAULT + relativedelta(seconds=-int(dat[0]))

class _Selector:
    def select_urls(self, xpaths, response=None):
        if not isinstance(xpaths, (list, tuple)):
            xpaths = [xpaths]

        return self._get_urls(response, xpaths)

    def _get_urls(self, response, xpaths):
        urls = [self.select(xpath) for xpath in xpaths]
        urls = list(chain(*urls))
        urls = [textify(u) for u in urls]
        if response:
            urls = [urljoin(response.url, u) for u in urls]
        return urls

class HTML(HtmlXPathSelector, _Selector):
    pass

class XML(XmlXPathSelector, _Selector):
    pass

def get_request_url(response):
    return response.meta.get('redirect_urls')[0] if response.meta.get('redirect_urls', []) else response.url

def gen_start_urls_from_db(spider_name, start_url=None, limit=2000, url_field='url'):
    items = get_uncrawled_pages(spider_name, limit=limit)
    if not items and start_url:
        if not isinstance(start_url, (tuple, list)):
            start_url = [start_url]

        items = [{url_field: u} for u in start_url]

    for item in items:
        yield item[url_field]

def get_uncrawled_pages(spider_name, limit=1):
    db, dbname = get_cursor()

    resp = db.find(dbname, "datastore", spec={"spider": spider_name, "data.got_page": False}, fields=["data"], limit=limit)
    records = []
    if resp.has_key("result"):
        records = [r["data"] for r in resp["result"]]
    return records

def lookup_items(spider_name, terms, limit=2000):
    if isinstance(terms, (unicode, str)):
        terms = [terms]

    db, dbname = get_cursor()

    resp = db.find(dbname, "data_index", spec={"spider": spider_name, "term":{"$in":terms}}, fields=["term", "data", "id"], limit=limit)

    records = [(r["id"], r["term"], r["data"]) for r in resp["result"]]

    return records

def empty_index(spider_name, term_pattern=None):
    db, dbname = get_cursor()

    if term_pattern:
        resp = db.remove(dbname, "data_index", {"spider": spider_name, "term": term_pattern})
    else:
        resp = db.remove(dbname, "data_index", {"spider": spider})

class JuicerStatsCollector(MemoryStatsCollector):

    def __init__(self):
        super(JuicerStatsCollector, self).__init__()
        self.db, self.dbname = get_cursor()

    def open_spider(self, spider):
        spider_run_id = id(spider)
        now = datetime.datetime.utcnow()

        resp = self.db.insert(self.dbname, "spider_run_stats", {"spider": spider.name, "run_id": spider_run_id,
                    "added": time.mktime(now.timetuple()) })

        frequency = None
        xtags = []
        mozrank = 0
        resp = self.db.find(self.dbname, "spiders", {"spider": spider.name})
        if resp and resp["result"]:
            frequency = resp["result"][0]["frequency"]
            xtags = resp["result"][0].get("xtags", [])
            mozrank = resp["result"][0].get("mozrank", 0)
            if frequency is not None:
                frequency = int(frequency)
                spider._latest_dt = get_datetime(resp['result'][0]['last_run'])

        next_run = None
        if frequency is not None:
            next_run = now + datetime.timedelta(seconds=frequency)
            next_run = time.mktime(next_run.timetuple())

        resp = self.db.find(self.dbname, "spiders", {"name": spider.schedule_name})
        priority = resp["result"][0].get("priority", 0) if resp["result"] else 0
        self.db.update(self.dbname, "spiders", {"name": spider.schedule_name},
            {"$set": {"last_run": time.mktime(now.timetuple()),
                "next_run": next_run, "effective_priority": priority}})

        spider.xtags = xtags
        spider.mozrank = mozrank
        super(JuicerStatsCollector, self).open_spider(spider)

    def close_spider(self, spider, reason):
        super(JuicerStatsCollector, self).close_spider(spider, reason)

    def _persist_stats(self, stats, spider=None):
        super(JuicerStatsCollector, self)._persist_stats(stats, spider)

        now = datetime.datetime.utcnow()
        (spider_name, spider_run_id) = (spider.schedule_name, id(spider)) if spider else ('', None)
        finish_time = stats.get('finish_time', time.mktime(now.timetuple()))
        finish_status = stats.get('finish_status', 'NOSTATS')
        items_scraped = stats.get('item_scraped_count', 0)
        items_passed = stats.get('item_passed_count', 0)

        resp = self.db.update(self.dbname, "spider_run_stats", {"spider": spider_name, "run_id":spider_run_id},
                {"$set":{"finish_time": str(finish_time),
                         "finish_status": finish_status,
                         "items_scraped": items_scraped,
                         "items_passed": items_passed,
                         "stats": repr(stats)}})

class JuicerSpider2(BaseSpider):
    priority = settings["DEFAULT_CRAWLER_PRIORITY"]
    def __init__(self, name=None, **kwargs):
        self._close_called = False
        dispatcher.connect(self._spider_closed, signals.spider_closed)
        #dispatcher.connect(self._spider_error, signals.spider_error)
        super(JuicerSpider2, self).__init__(name, **kwargs)

    """
    def _spider_error(self, spider, reason):
        if spider.name != self.name:
            return

        self.spider_error(reason)

    def spider_error(self, reason):
        pass
    """

    def _spider_closed(self, spider, reason):
        if spider.name != self.name:
            return

        if self._close_called: return False
        self._close_called = True
        self.spider_closed(spider, reason)

    def spider_closed(self, spider, reason):
        pass

class JuicerSpider(BaseSpider):
    def __init__(self, name=None, **kwargs):
        self.schedule_name = kwargs.get('_schedule_name', name)
        self._close_called = False
        self.limit_start_urls = 5
        self._adaptive_scheduling = False
        self._latest_dt = datetime.datetime(1900, 1, 1) # very old date

        if kwargs.get("LASTRUN"):
            self.latest_dt = get_datetime(float(kwargs.get("LASTRUN")))
            self.flag = False
        else:
            self.latest_dt = None
            self.flag = True

        self._start_urls = None
        if hasattr(self.__class__, 'start_urls'):
            self._start_urls = getattr(self.__class__, 'start_urls')
            try:
                delattr(self.__class__, 'start_urls')
            except AttributeError:
                pass

        dispatcher.connect(self._spider_closed, signals.spider_closed)
        super(JuicerSpider, self).__init__(name, **kwargs)

    def update_dt(self, dt, offset=None):
        if offset is not None:
            dt += offset

        self._latest_dt = max(self._latest_dt, dt)
        self._adaptive_scheduling = True

    def _spider_closed(self, spider, reason):
        if spider.name != self.name:
            return

        if self._close_called: return False
        self._close_called = True
        self.spider_closed(spider, reason)

        #FIXME: if adaptive, compute next run and update db
        if self._adaptive_scheduling:
            current_time_stamp = get_current_timestamp()
            latest_timestamp = get_timestamp(self._latest_dt)
            time_diff = abs(latest_timestamp - get_current_timestamp())
            if time_diff < ONE_HOUR:
                time_diff = ONE_HOUR
            elif time_diff > TWO_WEEKS:
                time_diff = TWO_WEEKS
            next_run = get_current_timestamp() + time_diff
            db_doc = {'$set': {'next_run': next_run, 'last_run': current_time_stamp }}
            db, dbname = get_cursor()
            resp = db.update(dbname, 'spiders', {'spider': self.name}, db_doc)
            print resp, self.name, db_doc, dbname

    def spider_closed(self, spider, reason):
        pass

    def start_requests(self):
        requests = []

        conn = _get_urlqueue(self.name)
        cursor = conn.cursor()
        cursor.execute('SELECT url, data FROM urlqueue WHERE got_page=0 LIMIT ?', (self.limit_start_urls,))

        records = cursor.fetchall()

        start_urls = self._start_urls or getattr(self, 'start_urls', None)

        if records:
            for url, data in records:
                if data:
                    data = eval(data)

                request = Request(url, self.parse, None, meta={'data': data}, priority=0)
                requests.extend(request)

        elif start_urls:
            _drop_urlqueue(self.name)

            urls = start_urls

            if not isinstance(urls, (tuple, list)) and not inspect.isgenerator(urls):
                urls = [urls]

            for url in urls:
                if isinstance(url, ScrapyHTTPRequest):
                    requests.append(url)
                else:
                    requests.extend(Request(url, self.parse, None, meta={'data': None}, priority=0))

        cursor.close()
        return requests

def xcode(text, encoding='utf8', mode='ignore'):
    return text.encode(encoding, mode) if isinstance(text, unicode) else text

def textify(nodes, sep=' '):
    if not isinstance(nodes, (list, tuple)):
        nodes = [nodes]

    def _t(x):
        if isinstance(x, (str, unicode)):
            return [x]

        if hasattr(x, 'xmlNode'):
            if not x.xmlNode.get_type() == 'element':
                return [x.extract()]
        else:
            if isinstance(x.root, (str, unicode)):
                return [x.root]

        return (n.extract() for n in x.select('.//text()'))

    nodes = chain(*(_t(node) for node in nodes))
    nodes = (node.strip() for node in nodes if node.strip())

    return sep.join(nodes)

class SpiderMiddleware(object):
    def process_spider_output(self, response, result, spider):
        if inspect.isgenerator(result):
            result = list(result)
            _result = []
            for r in result:
                if isinstance(r, (list, tuple)):
                    _result.extend(r)
                else:
                    _result.append(r)
            result = _result

        return result

def Request(url, callback=None, response=None, **kwargs):
    kwargs['callback'] = callback
    kwargs['meta'] = kwargs.get('meta', {})
    if settings['PROXIES_LIST']:
        kwargs['meta']['proxy'] = random.choice(settings['PROXIES_LIST'])
    urls = url if isinstance(url, (tuple, list)) else [url]
    out = []

    do_random_scheduling = settings.get('RANDOM_SCHEDULING', True)

    response_url = '' if response is None else response.url

    for url in urls:
        if not isinstance(url, (str, unicode)):
            url = url.extract()

        out.append(urlparse.urljoin(response_url, url))

    if do_random_scheduling and 'priority' not in kwargs:
        kwargs['priority'] = random.random()

    return [ScrapyHTTPRequest(u, **kwargs) for u in out]

class Item:
    def __init__(self, response, selector=HTML, spider=None, node=None):
        self.selector = node if node else selector(response)
        self.response = response

        self.data = {}
        self.sk = None
        self.spider = spider
        self.update_mode = None

    def _set_attr(self, attr, value):
        data = self.data
        attr_parts = attr.split('.')

        for index, attr_part in enumerate(attr_parts):
            _value = data.get(attr_part)
            if not isinstance(_value, dict):
                _value = data[attr_part] = {}

            if index == len(attr_parts) - 1:
                data[attr_part] = value
            else:
                data = _value

    def select(self, attr, xpath, fn=None):
        value = self.selector.select(xpath).extract()
        if fn is not None: value = fn(value)
        self._set_attr(attr, value)
        return value

    def textify(self, attr, xpath, fn=None, sep=' '):
        if not isinstance(xpath, (list, tuple)):
            xpath = [xpath]

        value = textify([textify(self.selector.select(x), sep) for x in xpath], sep)
        if fn is not None: value = fn(value)
        self._set_attr(attr, value)
        return value

    def set(self, attr, value, fn=None):
        if fn is not None: value = fn(value)
        self._set_attr(attr, value)
        return value

    def set_many(self, attrs):

        if isinstance(attrs, (list, tuple)):
            for attr in attrs:
                if isinstance(attr, dict):
                    attr_name = attr['attr']
                    value = attr['value']
                    fn = attr.get('fn', None)
                elif isinstance(attr, (list, tuple)):
                    if len(attr) == 2:
                        attr_name, value = attr
                    elif len(attr) == 3:
                        attr_name, value, fn = attr
                    else:
                        raise Exception('bad attrs: %s' % repr(attrs))
                else:
                    raise Exception('bad attrs: %s' % repr(attrs))

                self.set(attr_name, value, fn)

        elif isinstance(attrs, dict):
            for attr_name, value in attrs.iteritems():
                self.set(attr_name, value)

        else:
            raise Exception('bad attrs: %s' % repr(attrs))

        return self

    def json(self):
        return ensure_json(self.data)

    def process(self):
        item = JuicerItem()

        if self.spider is not None:
            item['spider'] = self.spider

        if self.update_mode is not None:
            item['update_mode'] = self.update_mode

        self.sk = self.sk or self.data.get('sk', None)
        if self.sk is None:
            self.sk = hashlib.md5(get_request_url(self.response)).hexdigest()

        if 'url' not in self.data:
            self.data['url'] = get_request_url(self.response)

        item['sk'] = self.sk.replace('.', '_')
        item['data'] = ensure_json(self.data)

        if item['data'].get('type') == 'common' or not item['data'].get('type'):
            validate_common(item)
        elif item['data'].get('type') == 'forum':
            validate_forum(item)
        else:
            raise Exception('Unsupported schema type : %s' % item['data'].get('type'))

        if item['data'].get('domain_name'):
            item['data'].pop('domain_name')

        return item

def validate_keys(_keys, item):
    keys = ['id', 'title', 'url']
    for key in keys:
        if key not in _keys:
            raise Exception('In forum dict key %s should be needed' %key)
        if key in _keys and not item['data'].get('forum')[key]:
            raise Exception('In forum dict for key %s value should not be empty' %key)

def validate_forum(item):

    if not isinstance(item.get('sk'), str):
        raise Exception('sk should be of type str but given %s' %type(item.get('sk')))

    if not isinstance(item['data'].get('url'), str):
        raise Exception('url should be of type str but given %s' %type(item['data'].get('url')))

    if isinstance(item['data'].get('url'), str) and 'http' not in item['data'].get('url'):
        raise Exception('url schema not supported:  %s' %item['data'].get('url'))

    if not isinstance(item['data'].get('text'), str):
        raise Exception('text should be of type str but given %s' %type(item['data'].get('text')))

    if not isinstance(item['data'].get('title'), str):
        raise Exception('title should be of type str but given %s' %type(item['data'].get('title')))

    if not item['data'].get('forum'):
        raise Exception('forum info dict should be present in an item')

    if not item['data'].get('thread'):
        raise Exception('thread info dict should be present in an item')

    if item['data'].get('forum'):
        validate_keys(item['data'].get('forum').keys(), item)

    if item['data'].get('thread'):
        validate_keys(item['data'].get('thread').keys(), item)

def validate_common(item):
    if not isinstance(item.get('sk'), str):
        raise Exception('sk should be of type str but given %s' %type(item.get('sk')))

    if not isinstance(item['data'].get('url'), str):
        raise Exception('url should be of type str but given %s' %type(item['data'].get('url')))

    if isinstance(item['data'].get('url'), str) and 'http' not in item['data'].get('url'):
        raise Exception('url schema not supported:  %s' %item['data'].get('url'))

    if not isinstance(item['data'].get('text'), str):
        raise Exception('text should be of type str but given %s' %type(item['data'].get('text')))

    if not isinstance(item['data'].get('title'), str):
        raise Exception('title should be of type str but given %s' %type(item['data'].get('title')))

def url(valid_regex, invalid_regex=None, fn=None):
    def _decorated_function(func):
        func._fn = fn

        func._valid_regex = [valid_regex] if isinstance(valid_regex, basestring) else valid_regex

        if not invalid_regex:
            func._invalid_regex = []
        else:
            func._invalid_regex = [invalid_regex] if isinstance(invalid_regex, basestring) else invalid_regex

        return func

    return _decorated_function

MAX_CRAWLER_PRIORITY = 9
MAX_RETRIES = 1
DELIMITER = "#"
INVALID_FETCH_AFTER = -1
MAX_EXPIRY_TIME = 86400 * 1
MIN_CURRENT_PRIORITY = 10
RSS_PRIORITY = 2
FORUM_PRIORITY = 3
TECHNORATI_PRIORITY = 3
FEED_TERMINAL_PRIORITY = 3

def _remove_defaults(_dict):
    d = {}
    for key, value in _dict.iteritems():
        if not value:
            continue

        d[key] = value

    return d

class URLSpiderData(object):
    FIELD_MAP = {"p": "priority", "da": "dt_added", "du": "dt_updated",
                 "g": "got_page", "dg": "dt_got_page", "m": "meta",
                 "fa": "fetch_after", "n": "name", "rm": "raw_meta" }

    def __init__(self, **kwargs):
        self.name = None
        self.priority = settings["DEFAULT_CRAWLER_PRIORITY"]
        self.dt_added = get_current_timestamp()
        self.dt_updated = get_current_timestamp()
        self.got_page = False
        self.dt_got_page = None
        self.meta = {}
        self.raw_meta = {}
        self.fetch_after = INVALID_FETCH_AFTER

        self._set(kwargs)

        if not self.name:
            raise Exception("Spider Name can't be empty")

    def _set(self, kwargs):
        for k, v in kwargs.iteritems():
            if hasattr(self, k):
                setattr(self, k, v)
            elif k in self.FIELD_MAP:
                setattr(self, self.FIELD_MAP[k], v)

    def crawled(self, completed=True):
        self.dt_updated = get_current_timestamp()
        self.got_page = completed
        if completed:
            self.dt_got_page = get_current_timestamp()

    def load(self, data):
        self._set(**data)

    def dump(self, show=False):

        data = _remove_defaults(dict((sr, getattr(self, ln)) for sr, ln in self.FIELD_MAP.iteritems()))
        if show:
            pprint.pprint(data)

        return data

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)

class URL(object):
    FIELD_MAP = {"u": "url", "pd": "post_data", "h": "headers",
        "id": "uid", "da": "dt_added", "du": "dt_updated",
        "df": "fetch_dates", "rt": "num_retries",
        "s": "spiders", "st": "state", "tid": "timeout_id",
        "m": "method", "eid": "eid", "fid": "fid",
        "v": "version", 'fc': 'fetch_count'}

    REV_FIELD_MAP = dict((v, k) for k, v in FIELD_MAP.iteritems())

    def __init__(self, **kwargs):
        self.uid = None
        self.url = None
        self.post_data = None
        self.headers = {}
        # TODO: add cookies as separate dict. This is inline with scrapy

        self.dt_added = get_current_timestamp()
        self.dt_updated = None
        self.fetch_dates = []
        self._priority = MAX_CRAWLER_PRIORITY
        self.num_retries = MAX_RETRIES
        self.spiders = {}
        self.state = 'FETCH'
        self.eid = None
        self.fid = None
        self.timeout_id = None
        self.method = None
        self.version = 1
        self.fetch_count = 0

        self._set(kwargs)

        if not self.uid:
            self.uid = self._get_id()

    @property
    def priority(self):
        return self._priority

    def update_priority(self):
        priorities = [spider.priority for _, spider in self.spiders.iteritems()]
        if priorities:
            self._priority = min(priorities)

    @staticmethod
    def from_request(scrapy_http_request):
        # Add meta and other information from scrapy req
        req = scrapy_http_request
        url = URL(url=req.url, post_data=req.body,
                headers=req.headers)
        return url

    def get_meta(self, spider):
        return getattr(self.spiders.get(spider), 'meta')

    def _set(self, kwargs):

        for k, v in kwargs.iteritems():
            if k == "priority":
                continue

            if hasattr(self, k):
                setattr(self, k, v)

            elif k in self.FIELD_MAP:
                setattr(self, self.FIELD_MAP[k], v)

        for name, spider in self.spiders.iteritems():
            if not isinstance(spider, URLSpiderData):
                #if not isinstance(spider, dict):
                #    spider = msgpack.loads(spider)

                spider = URLSpiderData(**spider)
                self.spiders[name] = spider

        self.update_priority()

    @staticmethod
    def load(data, _msgpack=False):
        if _msgpack:
            data = msgpack.loads(data)
        else:
            data = json.loads(data)

        url = URL(**data)
        return url

    def dump(self, _msgpack=False, show=False):
        self.fetch_dates = self.fetch_dates[:10]

        spiders = {}
        for name, spider in self.spiders.iteritems():
            spiders[name] = spider.dump()

        data = _remove_defaults(dict((sn, getattr(self, ln)) for sn, ln in self.FIELD_MAP.iteritems()))
        data['s'] = spiders

        if show:
            pprint.pprint(data)

        return msgpack.dumps(data) if _msgpack else json.dumps(data)

    def _get_id(self):
        headers = urllib.urlencode(self.headers)
        post_data = ""

        if self.post_data:
            post_data = urllib.urlencode(self.post_data) if not \
                    isinstance(self.post_data, basestring) else self.post_data

        data = []
        for i in [self.url, post_data, headers]:
            if isinstance(i, str):
                data.append(i.decode('utf8', 'ignore'))

        data = "".join(data)
        data = xcode(data)
        digest = hashlib.md5(data).hexdigest()
        _url = xcode(self.url)

        domain = urlparse.urlparse(_url).netloc
        domain = domain.split(".")
        domain.reverse()
        domain = ".".join(domain)

        return "%s%s%s"%(xcode(domain), DELIMITER, digest)

    def update_spider(self, spider):
         # Merge the old record with the new one.
        _spider = self.spiders[spider.name]

        _spider.priority = spider.priority
        _spider.dt_updated = spider.dt_updated
        _spider.got_page = spider.got_page
        _spider.dt_got_page = spider.dt_got_page
        _spider.fetch_after = spider.fetch_after
        _spider.raw_meta = spider.raw_meta
        # merge meta
        merge_meta(_spider.meta, spider.meta)

    def add_spider(self, spider, update=False):
        # TODO: Check for possible errors here.
        if spider.name not in self.spiders:
            self.spiders[spider.name] = copy.deepcopy(spider)
            self.update_priority()
            return

        if not update:
            return

        self.update_spider(spider)

    def reset_fetch_state(self):
        for _, spider in self.spiders.iteritems():
            spider.fetch_after = INVALID_FETCH_AFTER
        spider.fid = None
        spider.eid = None
        spider.timeout_id = None

    def __eq__(self, other):
            return self.__dict__ == other.__dict__

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)

def get_urldata(proxy, dbname, urls):
    uq = proxy.bulk_get(dbname, "url_queue", urls)
    uq = uq["result"] or {}
    if uq:
        uq = dict([(key, URL.load(data)) for key, data in uq.iteritems()])

    return uq

def get_event_id(priority, uid, ts=None):
    ts = get_current_timestamp() if not ts else ts
    return "%s%s%02d%s%s"%(ts, DELIMITER, int(priority), DELIMITER, xcode(uid))

def get_fetch_id(priority, sub_priority, uid, ts=None):
    ts = get_current_timestamp() if not ts else ts
    return DELIMITER.join(["%02d"%(int(priority)), "%02d"%(int(sub_priority)), str(ts), xcode(uid)])

MAX_PRIORITY = 0
def process_events(proxy, dbname, limit=5000):
    event_queue = []
    fetch_queue = {}

#    include_value=True, verify_checksums=False, fill_cache=True,
#        reverse=False, limit=10000

    key_to = "%s%s11"%(get_current_timestamp(), DELIMITER)
    events = proxy.range_query(dbname, "event_queue", include_value=True, limit=limit, key_to=key_to)
    events = events["result"]

    for eid, url_data in events:
        url = URL.load(url_data)

        # Construct fid from uid
        _ts, _prio, _uid = eid.split(DELIMITER, 2)
        fid = get_fetch_id(_prio, _uid, _ts)

        url.num_retries -= 1

        #fid = "%010d%s%s" % (url.priority, DELIMITER, url.uid)
        fetch_queue[fid] = [url.dump(), url.uid]

        event_queue.append([eid, url.uid])

    # put back in fetch queue
    result = proxy.bulk_put(dbname, "fetch_queue", fetch_queue)

    # remove from event_queue
    result = proxy.bulk_remove(dbname, "event_queue", event_queue)

    return len(event_queue)

def iter_classes(modules):
    classes = []
    for mod in modules:
        members = getmembers(mod, inspect.isclass)
        for name, obj in members:
            if issubclass(obj, JuicerSpider2) and \
                obj.__module__ == mod.__name__:
                classes.append(obj)

    return classes

def get_spider_name(spider):
    if not issubclass(spider.__class__, JuicerSpider2):
        return

    return getattr(spider, "name", spider.__class__.__name__)

def get_spiders(spider_modules):
    mods = []
    for name in spider_modules:
        for module in walk_modules(name):
            mods.append(module)

    spiders = []
    for obj in iter_classes(mods):
        cls = obj()

        spider_name = get_spider_name(cls)
        cls.set_crawler(spider_name)

        methods = getmembers(cls, inspect.ismethod)
        for name, method in methods:
            if getattr(method, "_valid_regex", None):
                break
        else:
            continue
        spiders.append(cls)

    return spiders

def get_spider_objects():
    spiders = get_spiders(settings.get("SPIDER_MODULES"))

    _spiders = {}
    for spider in spiders:
        name = get_spider_name(spider)
        methods = getmembers(spider, inspect.ismethod)
        _methods = []
        for _, method in methods:
            if not getattr(method, "_valid_regex", None):
                continue
            _methods.append(method)
        _spiders[name] = [spider, _methods]
    return _spiders

def _regex_matched(regexs, url):
    for regex in regexs:
        if re.match(regex, url):
            return True

    return False

def has_regex_matched(methods, url):
    _methods = methods if isinstance(methods, (list, tuple)) else [methods]

    for method in _methods:
        if _regex_matched(method._valid_regex, url.url) and \
            not _regex_matched(method._invalid_regex, url.url):
            if method._fn and not method._fn(url):
                return False

            return True

    return False

def getmembers(object, predicate=None):
    """Return all members of an object as (name, value) pairs sorted by name.
    Optionally, only return members that satisfy a given predicate."""
    results = []
    for key in dir(object):
        try:
            value = getattr(object, key)
        except AttributeError:
            continue
        if not predicate or predicate(value):
            results.append((key, value))
    results.sort()
    return results

def get_url_obj(response):
    return response.meta["_url"]

class RandomUserAgentMiddleware(object):

    def process_request(self, request, spider):
        ua  = random.choice(settings['USER_AGENT_LIST'])
        if ua:
            request.headers.setdefault('User-Agent', ua)

class CustomDownloaderStats(object):

    def __init__(self):
        update_stats = Counter()
        update_stats.statsd.host = settings['STATSD_HOST']
        self.update_stats = update_stats.statsd.update_stats
        self.key = settings['COUNTER_PREFIX'] + '.crawler.%s.' %socket.gethostname().replace('.', '_')


    def process_request(self, request, spider):
        self.start_time = time.time()
        self.update_stats(self.key+'requests.num')

    def process_response(self, request, response, spider):
        self.update_stats(self.key+'requests.time', time.time() - self.start_time)
        self.update_stats(self.key+'status.%s'%response.status)

        return response

def ensure_json(x):
    if isinstance(x, dict):
        for k, v in x.items():
            del x[k]
            k = ensure_json(k)
            x[k] = ensure_json(v)

    elif isinstance(x, list):
        for index, item in enumerate(x):
            x[index] = ensure_json(item)

    elif isinstance(x, datetime.datetime):
        x = get_timestamp(x)

    elif isinstance(x, unicode):
        x = x.encode('utf8', 'replace')

    return x

class DataHandler:
    def __init__(self, dump_dir, db='', db_name='', in_queue=''):
        self.dump_dir = dump_dir
        self.db = db
        self.db_name = db_name
        self.in_queue = in_queue

        self.start_time = None
        self.dump_file = None
        self.dump_fname = None

        self.name = ''.join([random.choice(string.letters) for x in range(10)])

    def store(self, data):
        if self.in_queue:
            return self._store_in_redis(data)
        elif self.dump_dir:
            return self._store_in_file(data)
        else:
            return self._store_in_db(data)

    def _store_in_db(self, data):
        return self.db.update(self.db_name, 'buzzinga', {'_id': data['_id']},
                    data, upsert=True)

    def _store_in_file(self, data):

        now = int(get_current_timestamp())
        self.start_time = self.start_time or now

        if now - self.start_time >= 60 or \
            not self.dump_file or \
            not self.dump_fname or \
            not os.path.exists(self.dump_fname):

            self.close()

            self.start_time = now
            self.dump_fname = '%012d_%d_%s.dump' % (self.start_time, os.getpid(), self.name)
            self.dump_fname = os.path.join(self.dump_dir, self.dump_fname)
            self.dump_file = open(self.dump_fname, 'a+')

        log.msg('pid=%s, writing to dump fname = %s' % (os.getpid(), self.dump_fname))
        self.dump_file.write('%s\n' % repr(data))
        self.dump_file.flush()

    def _store_in_redis(self, data):
        try:
            return self.in_queue.put(data)
        except redis.exceptions.ConnectionError:
            return "ERROR: Unable to Connect to Redis Server"
            sys.exit(1)
        except Exception as e:
            print "ERROR: %s" % e.message
            sys.exit(1)

    def close(self):
        if self.dump_file:
            self.dump_file.close()
            done_fname = '%s.done' % self.dump_fname
            shutil.move(self.dump_fname, done_fname)
            self.dump_file = None

def update_ttl(record, base_ttl):
    base_ttl_epoch = get_current_timestamp() - base_ttl/1000
    record_epoch = int(record['dt_added'])

    diff_ttl = record_epoch - base_ttl_epoch
    if record.has_key('_ttl') and record['_ttl'] is not None:
        record_ttl = conv_ttl_secs(record['_ttl'])
    else:
        record_ttl = base_ttl

    return min(record_ttl, diff_ttl)

def format_time(update):
    update = time.gmtime(float(update))
    return datetime.datetime(*update[:6]).isoformat()


def format_records(records, base_ttl, log):

    _records = []
    _ids = []

    for record in records:

        #add new ttl
        new_ttl = update_ttl(record, base_ttl)
        if new_ttl <= 0:
            continue
        record['_ttl'] = conv_secs_to_days_hours(new_ttl)

        try:
            tree = html.fromstring(record.get("text",""))
            tree = clean_html(tree)
            text = tree.text_content()
        except (SystemExit, KeyboardInterrupt):
            raise
        except:
            text = record.get("text", "")

        record["text"] = xcode(text)
        record["raw_string"] = "%s %s"%(xcode(record.get("title", '')), record["text"])
        record["tags"] = record.get("tags", [])
        record["xtags"] = record.get("xtags", [])
        record["_updated"] = format_time(record["_updated"])

        try:
            record["dt_added"] = format_time(record["dt_added"])
        except TypeError, e:
            record["dt_added"] = 0
            log.error("Dt Added Type error Raised")
        except Exception as e:
            log.error("%s", traceback.format_exc())

        dt_updated = record.get("dt_updated", '')
        if dt_updated:
            try:
                record["dt_updated"] = format_time(dt_updated)
            except TypeError, e:
                record["dt_updated"] = record["updated"]
                log.error("Dt Added Type error Raised")
            except Exception as e:
                log.error("%s", traceback.format_exc())
        else:
            record["dt_updated"] = record["updated"]

        _records.append(record)
        _ids.append(record.get("_id"))

    return _records, _ids

def conv_ttl_secs(ttl):
    max_secs = 365 * 86400

    if 'd' in ttl: secs = int(ttl.replace('d', '')) * 86400
    elif 'h' in ttl: secs = int(ttl.replace('h', '')) * 120

    if secs >= max_secs: secs = max_secs

    return secs

def conv_secs_to_days_hours(seconds):
    mins, secs = divmod(seconds, 60)
    hours, mins = divmod(mins, 60)
    days, hours = divmod(hours, 24)
    if hours >= 12: days += 1

    if days: return '%sd' % days
    if hours: return '%sh' % hours

    return '1h'
