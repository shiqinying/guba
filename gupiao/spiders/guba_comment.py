# -*- coding: utf-8 -*-
import json
from copy import deepcopy
from math import ceil

import pymongo
import scrapy
from scrapy.utils.project import get_project_settings

settings = get_project_settings()
DATABASE = settings['DATABASE']
COLLECTION_LIST = settings['COLLECTION_LIST']
COLLECTION_COMMENT = settings['COLLECTION_COMMENT']


class GubaCommentSpider(scrapy.Spider):
    name = 'guba_comment'
    allowed_domains = ['guba.eastmoney.com']
    start_urls = ['http://guba.eastmoney.com/']
    collection_list = pymongo.MongoClient()[DATABASE][COLLECTION_LIST]
    collection_comment = pymongo.MongoClient()[DATABASE][COLLECTION_COMMENT]

    def start_requests(self):
        for result in self.collection_list.find():
            result.pop('_id')
            try:
                comment_num = int(result['comment_num'])
            except:
                return
            postid = result['postid']
            if not postid:
                return
            total_page = ceil(comment_num / 30)
            for page in range(1, total_page + 1):
                url = 'https://guba.eastmoney.com/interface/GetData.aspx'
                formdata = {
                    'param': f'postid={postid}&sort=1&sorttype=1&p={page}&ps=30',
                    'path': 'reply/api/Reply/ArticleNewReplyList',
                    'env': '2'
                }
                headers = {
                    'Referer': result['detail_url'],
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
                pre_item = {}
                pre_item['name'] = result['name']
                pre_item['code'] = result['code']
                pre_item['stock_type'] = result['stock_type']
                pre_item['detail_url'] = result['detail_url']
                pre_item['title'] = result['title']
                pre_item['postid'] = result['postid']
                pre_item['comment_num'] = result['comment_num']
                pre_item['comment_page'] = page
                pre_item['postid_pageid'] = result['postid'] + '_' + str(page)
                yield scrapy.FormRequest(url=url, formdata=formdata, dont_filter=True, headers=deepcopy(headers),
                                         meta={'pre_item': deepcopy(pre_item)}, callback=self.parse_comment)

    def parse_comment(self, response):
        pre_item = response.meta['pre_item']
        comments = json.loads(response.text)['re']
        pre_item['comments'] = comments
        yield pre_item
