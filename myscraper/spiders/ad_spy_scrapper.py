import json
import scrapy
from scrapy.crawler import CrawlerProcess
import time
import datetime
from dateutil.relativedelta import relativedelta
from myscraper.mongo_db_connection import MongoDBConnectionClass

class MySpider(scrapy.Spider):
    name = "bigad"
    start_urls = ['https://bigspy.com']
    handle_httpstatus_list = [400]
    download_delay = 2

    # ad_category = "Real+Estate"
    ad_category = "Internet+Company"
    t = datetime.datetime.now()
    seen_begin = int((t + relativedelta(months=-3)).timestamp())
    seen_end = int(t.timestamp())
    def parse(self, response):
        # big_ad = "https://bigspy.com/ads/get-facebook-ads?position=&keyword=&exclude_keyword=&campaign_type=&industry=&advertiser_type=&category=&geo=USA&os=&language=en&seen_begin=1585335600&seen_end=1593197999&created_time_begin=&created_time_end=&last_seen_begin=&last_seen_end=&site_type=&page=1&cta_type=&type=&sort=1&is_first=1&affiliate_network=&affiliate_id=&offer_id=&like_begin=&like_end="
        # big_ad = "https://bigspy.com/ads/get-facebook-ads?position=&keyword=&exclude_keyword=&campaign_type=&industry=&advertiser_type=&category=&geo=USA&os=&language=&seen_begin=1585681200&seen_end=1593543599&created_time_begin=&created_time_end=&last_seen_begin=&last_seen_end=&site_type=&page=1&cta_type=&type=&sort=1&is_first=1&affiliate_network=&affiliate_id=&offer_id=&like_begin=&like_end="

        big_ad_url = "https://bigspy.com/ads/get-facebook-ads?" \
                 "position=&" \
                 "keyword=&" \
                 "exclude_keyword=&" \
                 "campaign_type=" \
                 "&industry=&" \
                 "advertiser_type=&" \
                 f"category={self.ad_category}&" \
                 "geo=USA&" \
                 "os=&" \
                 "language=&" \
                 f"seen_begin={self.seen_begin}&" \
                 f"seen_end={self.seen_end}&" \
                 "created_time_begin=&" \
                 "created_time_end=&" \
                 "last_seen_begin=&" \
                 "last_seen_end=&" \
                 "site_type=&" \
                 "cta_type=&" \
                 "type=&" \
                 "sort=1&" \
                 "is_first=1&" \
                 "affiliate_network=&" \
                 "affiliate_id=&" \
                 "offer_id=&" \
                 "like_begin=&" \
                 "like_end=&" \
                 f"page={self.page}"
        # url = response.url.split('?')[0] + '/async/search_ads/?count=30&active_status=all&ad_type=all&countries[0]=PK&impression_search_field=has_impressions_lifetime&view_all_page_id=503606332982752&sort_data[direction]=desc&sort_data[mode]=relevancy_monthly_grouped'
        yield scrapy.Request(big_ad_url, cookies=self.get_cookies(), callback=self.parse_page)

    def parse_page(self, response):
        res = response.body.decode("utf-8")
        res = json.loads(res)
        data = res['data']
        insert_data(data, self.ad_category)

    def get_cookies(self):
        return {
            '_ga': 'GA1.2.712664313.1591556929',
            '__stripe_mid': 'a73b4c50-a2d7-48b6-9fbc-213cc20feff2',
            '_gid': 'GA1.2.589211758.1593456835',
            'crisp-client%2Fsession%2Febd7cf0f-b1ee-4a4e-a4ce-8064999c1331': 'session_aa7bc8ee-e544-4579-b007-533fd0d79254',
            'crisp-client%2Fsocket%2Febd7cf0f-b1ee-4a4e-a4ce-8064999c1331': '1',
            '_csrf': 'd2a70a0eca1b17f1ae03c0749c7c1d11fe9896503d30449dfb9a7521121f2005a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22jE2No2KQy74aM-uwiQ3ygLEffW8Wnqsj%22%3B%7D',
            'timezone': '-300',
            'last_login': 'cbccb035d77dd3a3bf16294421cd2706f4a8f02494c3fadfa08968751dd9eefca%3A2%3A%7Bi%3A0%3Bs%3A10%3A%22last_login%22%3Bi%3A1%3Bs%3A0%3A%22%22%3B%7D',
            '_trackUserId': 'G-1593459076000',
            'zbase_popup_26': 'e1e66405c90091276ac1b405af8fcdc4407d0cc4a2a27860096f0a8502a8ae80a%3A2%3A%7Bi%3A0%3Bs%3A14%3A%22zbase_popup_26%22%3Bi%3A1%3Bs%3A9%3A%22isPopup26%22%3B%7D',
            '__stripe_sid': 'eb79c3a0-8fd8-4bd6-a2db-9a8384eb764c',
            'zbase_popup_': 'b5bb042bc89e331370a1b1c0d9dd39f6522e70f0a082369a71cef82583c4be8fa%3A2%3A%7Bi%3A0%3Bs%3A12%3A%22zbase_popup_%22%3Bi%3A1%3Bs%3A7%3A%22isPopup%22%3B%7D',
            'zbase_popup_12': 'bdfb2aa86df83f81401f961b870be36315b12142976c092549334a222fb3177da%3A2%3A%7Bi%3A0%3Bs%3A14%3A%22zbase_popup_12%22%3Bi%3A1%3Bs%3A9%3A%22isPopup12%22%3B%7D',
            'ZFSESSID': 'j5jl64vka62a8ihgnpj15f21b5',
            '_identity': '809a8728a4465c9a7b39bf42155b15a4746945733bdc444083277eaae4878863a%3A2%3A%7Bi%3A0%3Bs%3A9%3A%22_identity%22%3Bi%3A1%3Bs%3A19%3A%22%5B420737%2Cnull%2C86400%5D%22%3B%7D',
            '_gat_gtag_UA_121710730_2': '1',
            'timezone_key': '14740b22Tgry6zco86w',
            '_gat': '1',
            'SERVERID': '5dfe92cc422d185f34a1898663840774|1593467114|1593467114'
        }


def insert_data(data, category):
    connection = MongoDBConnectionClass()
    processDb = connection.connectToMongo()
    adCol = processDb.ads

    data = [dict(item, ad_category=category) for item in data]
    adCol.insert(data)
    print("record inserted")

# insert_data({"id": 2, "name": "ali"})


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

for i in range(83):
    process.crawl(MySpider, page=i+1)

process.start()
