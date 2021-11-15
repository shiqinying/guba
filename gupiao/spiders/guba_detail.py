# -*- coding: utf-8 -*-
import re
from copy import deepcopy

import pymongo
import scrapy
from scrapy.utils.project import get_project_settings

settings = get_project_settings()
DATABASE = settings['DATABASE']
COLLECTION_LIST = settings['COLLECTION_LIST']
COLLECTION_DETAIL = settings['COLLECTION_DETAIL']


class GubaDetailSpider(scrapy.Spider):
    name = 'guba_detail'
    allowed_domains = ['guba.eastmoney.com']
    start_urls = ['http://guba.eastmoney.com/']
    collection_list = pymongo.MongoClient()[DATABASE][COLLECTION_LIST]
    collection_detail = pymongo.MongoClient()[DATABASE][COLLECTION_DETAIL]

    def start_requests(self):
        for result in self.collection_list.find():
            result.pop('_id')
            if self.collection_detail.find_one({'post_url_id':result['post_url_id']}):
                # print('数据已存在：',result['post_url_id'])
                continue
            yield scrapy.Request(url=result['detail_url'], dont_filter=True, callback=self.parse_detail,
                                 meta={'pre_item': deepcopy(result)})

    def parse_detail(self, response):
        pre_item = response.meta['pre_item']
        content = response.xpath('string(//*[@id="post_content"])').get('')
        if not content:
            content = response.xpath('string(//*[@class="zwcontentmain"])').get('')
            if not content:
                content = response.xpath('string(//*[@id="mian-content-wrap"])').get('')

        pre_item['content'] = content.replace(pre_item['title'], '', 1).replace('\n', '').replace('\r', '').replace(
            '\t', '').replace(' ', '')
        # pre_item['content_images'] = response.xpath('//*[@id="post_content"]//img/@src').getall()
        # content_html = response.xpath('//*[@id="post_content"]').get('')
        # pre_item['content_html_base64'] =  base64.b64encode(content_html.encode("utf-8"))
        pub_time = response.xpath('//*[@class="zwfbtime"]/text()').get('').strip()
        pre_item['pub_time_2'] = pub_time
        pre_item['pub_time_3'] = ''.join(re.findall('(\d+-\d+-\d+ \d+:\d+:\d+)', pub_time))
        print(pre_item)
        yield pre_item
