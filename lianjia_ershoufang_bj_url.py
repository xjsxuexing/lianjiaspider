#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = ['xjs <jingsheng_xu@bacic5i5j.com>']
import requests
import lxml.html
import pymysql
import time

#对链家北京二手房列表页进行爬取
#由于链家PC端并不显示所有房源，所以选择从WAP端下手
class Lianjia_Ershoufang(object):
    def __init__(self):
        self.ershoufang_url = []
        self.urls = []

    def get_url(self):
        #生成北京二手房列表页的URL
        for i in range(1,11,1):
            url = 'https://m.lianjia.com/bj/ershoufang/pg{}/'.format(i)
            self.urls.append(url)
            #print(self.urls)

    def get_res(self):
        #解析网页，获取列表页上个房源详情页的URL
        for url_ls in self.urls:
            res = requests.get(url_ls).content
            selector = lxml.html.fromstring(res)
            link = selector.xpath('//li[@class="pictext"]/a/@href')
            for links in link:
                self.ershoufang_url.append(links)
            time.sleep(5)
            print(self.ershoufang_url)




    def to_mysql(self):
        #将抓取的内容存入到MYSQL数据库中
        conn = pymysql.connect(host='******', user='root', passwd='*********', db='mysql', use_unicode=True,
                               charset="utf8")
        cur = conn.cursor()
        cur.execute("USE lianjia")
        url_status = '2'     #在库表中多加一个判别状态的字段来实现增量爬取
        for ershoufang_urls in self.ershoufang_url:
            try:
                sql = "INSERT INTO lianjia_ershoufang_bj_url(ershoufang_url,url_status) VALUE (%s,%s)"
                cur.execute(sql,(ershoufang_urls,url_status))
                cur.connection.commit()
            except:
                pass
        cur.close()
        conn.close()

if __name__ == '__main__':
    lianjia_ershoufang  = Lianjia_Ershoufang()
    lianjia_ershoufang.get_url()
    lianjia_ershoufang.get_res()
    lianjia_ershoufang.to_mysql()
