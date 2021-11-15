# -*- coding: utf-8 -*-
import hashlib
import re
from urllib.parse import urljoin

import scrapy
from scrapy.utils.project import get_project_settings

settings = get_project_settings()
DATABASE = settings['DATABASE']
COLLECTION_LIST = settings['COLLECTION_LIST']
COLLECTION_DETAIL = settings['COLLECTION_DETAIL']
COLLECTION_COMMENT = settings['COLLECTION_COMMENT']


class GubaDetailSpider(scrapy.Spider):
    name = 'guba_list'
    allowed_domains = ['guba.eastmoney.com']
    start_urls = ['http://guba.eastmoney.com/']

    def start_requests(self):
        # https://guba.eastmoney.com/list,zssh000001_2.html
        start = 8108
        end = 33040
        for page in range(start - 1000, end + 1000):
            url = f'https://guba.eastmoney.com/list,zssh000001_{page}.html'
            yield scrapy.Request(url=url, dont_filter=True, callback=self.parse_list, meta={'page': page})

    def parse_list(self, response):
        page = response.meta['page']
        for div in response.xpath('//div[@id="articlelistnew"]/div[starts-with(@class,"articleh")]'):
            item = {}
            item['page'] = page
            item['list_url'] = response.url
            detail_url = div.xpath('./*[@class="l3 a3"]/a/@href').get('').strip()
            item['detail_url'] = urljoin('https://guba.eastmoney.com/', detail_url)
            try:
                item['postid'] = re.findall('\d+', detail_url, re.S)[-1]
            except:
                item['postid'] = ''
            item['title'] = div.xpath('./*[@class="l3 a3"]/a/text()').get('').strip()
            item['post_type'] = div.xpath('./*[@class="l3 a3"]/em//text()').getall()
            item['post_url_id'] = hashlib.md5(item['detail_url'].encode('utf8')).hexdigest()
            item['author'] = div.xpath('./*[@class="l4 a4"]/a/font/text()').get('').strip()
            item['author_type'] = div.xpath('./*[@class="l4 a4"]/a/em/@title').getall()
            item['author_url'] = "https:" + div.xpath('./*[@class="l4 a4"]/a/@href').get('').strip()
            item['author_id'] = item['author_url'].split('/')[-1]

            item['read_num'] = div.xpath('./*[@class="l1 a1"]/text()').get('').strip()
            item['comment_num'] = div.xpath('./*[@class="l2 a2"]/text()').get('').strip()
            item['pub_time'] = div.xpath('./*[@class="l5 a5"]/text()').get('').strip()

            yield item
