# -*- coding: utf-8 -*-
import hashlib
from copy import deepcopy
from math import ceil
from urllib.parse import urljoin
import re
import pymongo
import scrapy
from scrapy.utils.project import get_project_settings

settings = get_project_settings()
DATABASE = settings['DATABASE']
COLLECTION_LIST_COUNT = settings['COLLECTION_LIST_COUNT']
COLLECTION_LIST = settings['COLLECTION_LIST']
COLLECTION_DETAIL = settings['COLLECTION_DETAIL']
COLLECTION_COMMENT = settings['COLLECTION_COMMENT']


class GubaDetailSpider(scrapy.Spider):
    name = 'guba_list_count'
    allowed_domains = ['guba.eastmoney.com']
    start_urls = ['http://guba.eastmoney.com/']
    # 个股吧(沪市,深市),1,沪市,2深市
    stock_type_list = [1, 2]
    stock_filter = []
    collection_list_count = pymongo.MongoClient()[DATABASE][COLLECTION_LIST_COUNT]

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
            if self.collection_list_count.find({'code':pre_item['code']}):
                print(f'exits:{pre_item["code"]}')
                continue
            pre_item['stock_type'] = stock_type
            url = f'https://guba.eastmoney.com/list,{pre_item["code"]},f_{page}.html'
            yield scrapy.Request(url=url, dont_filter=True, callback=self.parse_list,
                                 meta={'pre_item': deepcopy(pre_item), 'page': page})

    def parse_list(self, response):
        pre_item = response.meta['pre_item']
        page = response.meta['page']
        if page == 1:
            try:
                # <span class="pagernums" data-pager="list,603099_|34715|80|1"></span> |总条数|每页条数|第几页
                total_item = response.xpath('//span[@class="pagernums"]/@data-pager').re('\|(\d+)\|80\|')[0]
            except:
                total_item = 0
            total_page = ceil(int(total_item) / 80)
            item = {}
            item.update(pre_item)
            item['total_item'] = total_item
            item['total_page'] = total_page
            yield item
