# -*- coding: utf-8 -*-
import scrapy
from urllib.parse import urljoin
from datetime import timedelta, date
import base64
import re
import hashlib
import pymongo
from copy import deepcopy
from math import ceil
import json


class GubaCommentSpider(scrapy.Spider):
    name = 'guba_comment_reply'
    allowed_domains = ['guba.eastmoney.com']
    start_urls = ['http://guba.eastmoney.com/']
    collection_comment = pymongo.MongoClient()['股票']['股吧_comment']
    collection_comment_reply = pymongo.MongoClient()['股票']['股吧_comment_reply']

    def start_requests(self):
        for result in self.collection_comment.find():
            if not result['comments']:
                continue
            for comment in result['comments']:
                if comment['reply_count']>2:
                    total_page = ceil(comment['reply_count'] / 10)
                    for page in range(1,total_page+1):
                        pre_item = {}
                        pre_item['postid'] = result["postid"]
                        pre_item['reply_id'] = comment["reply_id"]
                        pre_item['reply_count'] = comment['reply_count']
                        pre_item['page'] = page
                        pre_item['id'] = str(comment["reply_id"])+'_'+str(page)

                        if self.collection_comment_reply.find_one({'id': pre_item['id']}):
                            print('>>>>>>数据已处理:', pre_item['id'])
                            continue
                        url = 'https://guba.eastmoney.com/interface/GetData.aspx'
                        formdata = {
                            'param': f'postid={result["postid"]}&replyid={comment["reply_id"]}&sort=1&sorttype=1&ps=10&p={page}',
                            'path': 'reply/api/Reply/ArticleReplyDetail',
                            'env': '2'
                        }
                        headers = {
                            'Referer': result['detail_url'],
                            'Content-Type': 'application/x-www-form-urlencoded'
                        }
                        yield scrapy.FormRequest(url=url, formdata=formdata, dont_filter=True, headers=deepcopy(headers),
                                                 meta={'pre_item': deepcopy(pre_item)})


    def parse(self, response):
        pre_item = response.meta['pre_item']
        comment_replys = json.loads(response.text)['re']
        pre_item['comment_replys'] = comment_replys
        # print(pre_item)
        yield pre_item
