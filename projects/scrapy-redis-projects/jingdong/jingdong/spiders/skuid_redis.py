from scrapy_redis.spiders import RedisSpider
from scrapy.http import Request

class SkuIdSpider(RedisSpider):
    """Spider that reads urls from redis queue (myspider:start_urls)."""
    name = 'jdskuid'
    #redis_key = 'jdskuid:start_urls'
    allowed_domains = ['baidu.com']
    start_urls = ['http://www.baidu.com']

    def __init__(self, *args, **kwargs):
        # Dynamically define the allowed domains list.
        domain = kwargs.pop('domain', '')
        self.allowed_domains = filter(None, domain.split(','))
        super(SkuIdSpider, self).__init__(*args, **kwargs)

    def parse(self, response):
        self.log(response.url)
        yield Request(response.url, callback=self.parse, dont_filter=True)
