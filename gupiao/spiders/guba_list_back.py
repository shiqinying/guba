# -*- coding: utf-8 -*-
import hashlib
from copy import deepcopy
from math import ceil
from urllib.parse import urljoin
import re
import scrapy
from scrapy.utils.project import get_project_settings

settings = get_project_settings()
DATABASE = settings['DATABASE']
COLLECTION_LIST = settings['COLLECTION_LIST']
COLLECTION_DETAIL = settings['COLLECTION_DETAIL']
COLLECTION_COMMENT = settings['COLLECTION_COMMENT']


class GubaDetailSpider(scrapy.Spider):
    name = 'guba_list_back'
    allowed_domains = ['guba.eastmoney.com']
    start_urls = ['http://guba.eastmoney.com/']
    # 个股吧(沪市,深市),1,沪市,2深市
    stock_type_list = [1, 2]
    stock_filter = ['605168']

    def start_requests(self):
        for stock_type in self.stock_type_list:
            url = f'https://guba.eastmoney.com/remenba.aspx?type=1&tab={stock_type}'
            yield scrapy.Request(url=url, dont_filter=True, callback=self.parse_code, meta={'stock_type': stock_type})

    def parse_code(self, response):
        stock_type = response.meta['stock_type']
        page = 1
        # 个股吧
        for li in response.xpath('//div[@class="ngbglistdiv"]/ul/li'):
            pre_item = {}
            text = li.xpath('./a/text()').get('').strip()
            pre_item['name'] = text.split(')')[-1]
            pre_item["code"] = text.split(')')[0].replace('(', '')
            if self.stock_filter and pre_item['code'] not in self.stock_filter:
                continue
            pre_item['stock_type'] = stock_type
            url = f'https://guba.eastmoney.com/list,{pre_item["code"]},f_{page}.html'
            yield scrapy.Request(url=url, dont_filter=True, callback=self.parse_list,
                                 meta={'pre_item': deepcopy(pre_item), 'page': page})

    def parse_list(self, response):
        pre_item = response.meta['pre_item']
        page = response.meta['page']
        for div in response.xpath('//div[@id="articlelistnew"]/div[starts-with(@class,"articleh")]'):
            item = {}
            item.update(pre_item)
            item['page'] = page
            item['list_url'] = response.url
            detail_url = div.xpath('./*[@class="l3 a3"]/a/@href').get('').strip()
            item['detail_url'] = urljoin('https://guba.eastmoney.com/', detail_url)
            try:
                item['postid'] = re.findall('\d+',detail_url, re.S)[-1]
            except:
                item['postid'] =''
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
        if page == 1:
            try:
                # <span class="pagernums" data-pager="list,603099_|34715|80|1"></span> |总条数|每页条数|第几页
                total_item = response.xpath('//span[@class="pagernums"]/@data-pager').re('\|(\d+)\|80\|')[0]
            except:
                total_item = 0
            total_page = ceil(int(total_item) / 80)
            for page in range(2, total_page + 1):
                url = f'https://guba.eastmoney.com/list,{pre_item["code"]},f_{page}.html'
                yield scrapy.Request(url=url, dont_filter=True, callback=self.parse_list,
                                     meta={'pre_item': deepcopy(pre_item), 'page': page})
