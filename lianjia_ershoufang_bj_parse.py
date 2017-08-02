#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = ['xjs <jingsheng_xu@bacic5i5j.com>']

import requests
import lxml.html
import pymysql
import re
import time

#将抓取来的URL从MYSQL中提取出来，进行解析
class Lianjia_Homepage_Parse(object):
    def __init__(self):
        self.parse_url = []
        self.parse_url_update = []
        self.house_title = []
        self.house_price = []
        self.house_average = []
        self.house_listing_time = []
        self.house_orientation = []
        self.house_floor = []
        self.house_form = []
        self.house_elevator = []
        self.house_fitment = []
        self.house_age = []
        self.house_purpose = []
        self.house_ownership = []
        self.house_payment = []
        self.house_name = []
        self.house_type = []
        self.house_area = []
        self.house_inside = []
        self.house_structure = []
        self.house_elevator2 = []
        self.house_heating = []
        self.house_last_time = []
        self.house_age_limit = []
        self.house_equity = []
        self.house_pledge = []
        self.house_permit = []
        self.house_looktime = []
        self.house_lianjia_id = []
        self.house_land_time = []

    def extract_url(self):
        #从数据库中抽取未解析过的URL
        conn = pymysql.connect(host='******', user='root', passwd='*********', db='mysql', use_unicode=True,
                               charset="utf8")
        cur = conn.cursor()
        cur.execute('USE lianjia')
        sql = "SELECT ershoufang_url FROM lianjia_ershoufang_bj_url WHERE url_status = '2'"
        cur.execute(sql)
        result = cur.fetchall()  # 获取查询结果，结果的数据类型为tuple
        for result2 in result:
            for result3 in result2:
                result4 = "https://m.lianjia.com" + result3 #将URL拼接为完整的链接
                self.parse_url.append(result4)
                self.parse_url_update.append(result3)
        cur.connection.commit()
        cur.close()
        conn.close()
        print(self.parse_url)

    def parse_link(self):
        for parse_url_l in self.parse_url:
            time.sleep(3)
            res = requests.get(parse_url_l).content
            selector = lxml.html.fromstring(res)
            try:
                house_titles = selector.xpath('//h3[@class="house_desc"]/text()')[0]  # 房源标题
                house_prices = selector.xpath('//p[@class="red big"]/span/text()')[0]  # 房屋售价
                house_averages = selector.xpath('//li[@class="short"]/text()')[0]  # 房屋均价
                house_listing_times = selector.xpath('//li[@class="short"]/text()')[1]  # 房屋挂牌时间
                house_orientations = selector.xpath('//li[@class="short"]/text()')[2]  # 房屋朝向
                house_floors = selector.xpath('//li[@class="short"]/text()')[3]  # 房屋楼层
                house_forms = selector.xpath('//li[@class="short"]/text()')[4]  # 房屋楼型（如：板楼）
                house_elevators = selector.xpath('//li[@class="short"]/text()')[5]  # 电梯
                house_elevator2s = selector.xpath('//p[@class="info_content deep_gray"]/text()')[4]  # 梯户比例
                house_fitments = selector.xpath('//li[@class="short"]/text()')[6]  # 装修
                house_ages = selector.xpath('//li[@class="short"]/text()')[7]  # 年代
                house_purposes = selector.xpath('//li[@class="short"]/text()')[8]  # 房屋用途（如：普通住宅）
                house_ownerships = selector.xpath('//li[@class="short"]/text()')[9]  # 房屋权属（如：商品房）
                try:
                    house_payments = selector.xpath('//li[@class="long "]/a/text()')[0]  # 首付预算   #class=long后面有个空格
                except IndexError:
                    house_payments = selector.xpath('//li[@class="long  arrow "]/a/text()')[0]
                try:
                    house_names = selector.xpath('//li[@class="long "]/a/text()')[1]  # 小区名称(有些页面小区名称处有一个链接）
                except IndexError:
                    try:
                        house_names = selector.xpath('//li[@class="long  arrow "]/a/text()')[1]
                    except IndexError:
                        house_names = selector.xpath('//li[@class="long  arrow "]/a/text()')[0]
                house_types = selector.xpath('//p[@class="info_content deep_gray"]/text()')[0]  # 房屋户型
                house_areas = selector.xpath('//p[@class="info_content deep_gray"]/text()')[1]  # 房屋建筑面积
                house_insides = selector.xpath('//p[@class="info_content deep_gray"]/text()')[2]  # 房屋套内面积
                house_structures = selector.xpath('//p[@class="info_content deep_gray"]/text()')[3]  # 房屋户型结构
                house_heatings = selector.xpath('//p[@class="info_content deep_gray"]/text()')[5]  # 供暖方式
                house_last_times = selector.xpath('//p[@class="info_content deep_gray"]/text()')[6]  # 上次交易时间
                house_age_limits = selector.xpath('//p[@class="info_content deep_gray"]/text()')[7]  # 购房年限
                house_equitys = selector.xpath('//p[@class="info_content deep_gray"]/text()')[9]  # 产权所属
                house_pledges = selector.xpath('//p[@class="info_content deep_gray"]/text()')[11]  # 抵押信息
                house_permits = selector.xpath('//p[@class="info_content deep_gray"]/text()')[12]  # 房本备件
                house_looktimes = selector.xpath('//p[@class="info_content deep_gray"]/text()')[13]  # 看房时间
                house_lianjia_ids = selector.xpath('//p[@class="info_content deep_gray"]/text()')[14]  # 链家编号
                house_land_times = selector.xpath('//p[@class="info_content deep_gray"]/text()')[15]  # 土地年限
                self.house_title.append(house_titles)
                for text in re.findall('\d+',house_prices):
                    self.house_price.append(text)
                self.house_average.append(house_averages)
                self.house_listing_time.append(house_listing_times)
                self.house_orientation.append(house_orientations)
                self.house_floor.append(house_floors)
                self.house_form.append(house_forms)
                self.house_elevator.append(house_elevators)
                self.house_elevator2.append(house_elevator2s)
                self.house_fitment.append(house_fitments)
                self.house_age.append(house_ages)
                self.house_purpose.append(house_purposes)
                self.house_ownership.append(house_ownerships)
                self.house_payment.append(house_payments)
                self.house_name.append(house_names)
                self.house_type.append(house_types)
                for text2 in re.findall('\d+.\d+', house_areas):
                    self.house_area.append(text2)
                if house_insides == u'暂无数据':  #在Ubuntu上运行会提示编码错误，要在字符串前加个u
                    house_insides = '0'
                    self.house_inside.append(house_insides)
                else:
                    for text3 in re.findall('\d+.\d+', house_insides):
                        self.house_inside.append(text3)
                self.house_structure.append(house_structures)
                self.house_heating.append(house_heatings)
                self.house_last_time.append(house_last_times)
                self.house_age_limit.append(house_age_limits)
                self.house_equity.append(house_equitys)
                self.house_pledge.append(house_pledges)
                self.house_permit.append(house_permits)
                self.house_looktime.append(house_looktimes)
                self.house_lianjia_id.append(house_lianjia_ids)
                self.house_land_time.append(house_land_times)
            except:
                pass

    def output_mysql(self):
        conn = pymysql.connect(host='******', user='root', passwd='*********', db='mysql', use_unicode=True,
                               charset="utf8")
        cur = conn.cursor()
        cur.execute("USE lianjia")
        for house_title_l,house_price_l,house_average_l,house_listing_time_l,house_orientation_l,house_floor_l,house_form_l,\
            house_elevator_l,house_elevator2_l,house_fitment_l,house_age_l,house_purpose_l,house_ownership_l,house_payment_l,\
            house_name_l,house_type_l,house_area_l,house_inside_l,house_structure_l,house_heating_l,house_last_time_l,house_age_limit_l,\
            house_equity_l,house_pledge_l,house_permit_l,house_looktime_l,house_lianjia_id_l,house_land_time_l in zip\
            (self.house_title,self.house_price,self.house_average,self.house_listing_time,self.house_orientation,self.house_floor,self.house_form,
            self.house_elevator,self.house_elevator2,self.house_fitment,self.house_age,self.house_purpose,self.house_ownership,self.house_payment,
            self.house_name,self.house_type,self.house_area,self.house_inside,self.house_structure,self.house_heating,self.house_last_time,self.house_age_limit,
            self.house_equity,self.house_pledge,self.house_permit,self.house_looktime,self.house_lianjia_id,self.house_land_time):

            #print(house_inside_l)
            try:
                sql = "INSERT INTO lianjia_ershoufang_bj_parse(house_lianjia_id,house_title,house_price,house_average,house_listing_time,house_orientation,house_floor,house_form,house_elevator,house_elevator2,house_fitment,house_age,house_purpose,house_ownership,house_payment,house_name,house_type,house_area,house_inside,house_structure,house_heating,house_last_time,house_age_limit,house_equity,house_pledge,house_permit,house_looktime,house_land_time) VALUE (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                cur.execute(sql,(house_lianjia_id_l,house_title_l,house_price_l,house_average_l,house_listing_time_l,house_orientation_l,house_floor_l,house_form_l,\
                house_elevator_l,house_elevator2_l,house_fitment_l,house_age_l,house_purpose_l,house_ownership_l,house_payment_l,\
                house_name_l,house_type_l,house_area_l,house_inside_l,house_structure_l,house_heating_l,house_last_time_l,house_age_limit_l,\
                house_equity_l,house_pledge_l,house_permit_l,house_looktime_l,house_land_time_l))
                cur.connection.commit()
            except:
                pass
        for parse_url_update_l in self.parse_url_update:
            sql2 = "UPDATE lianjia_ershoufang_bj_url SET url_status = 1 WHERE ershoufang_url = (%s)"
            cur.execute(sql2, parse_url_update_l)
            cur.connection.commit()
        cur.close()
        conn.close()

if __name__ == '__main__':
     lianjia_homepage_parse = Lianjia_Homepage_Parse()
     lianjia_homepage_parse.extract_url()
     lianjia_homepage_parse.parse_link()
     lianjia_homepage_parse.output_mysql()
