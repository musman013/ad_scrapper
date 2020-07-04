import json
import scrapy
from scrapy.crawler import CrawlerProcess
from myscraper.mongo_db_connection import MongoDBConnectionClass


class MySpider(scrapy.Spider):
    name = "my spider"
    start_urls = ['https://web.facebook.com/ads/library']

    def parse(self, response):
        print("d")
        # big_ad = "https://bigspy.com/ads/get-facebook-ads?position=&keyword=&exclude_keyword=&campaign_type=&industry=&advertiser_type=&category=&geo=USA&os=&language=en&seen_begin=1585335600&seen_end=1593197999&created_time_begin=&created_time_end=&last_seen_begin=&last_seen_end=&site_type=&page=1&cta_type=&type=&sort=1&is_first=1&affiliate_network=&affiliate_id=&offer_id=&like_begin=&like_end="
        url = response.url.split('?')[0] + '/async/search_ads/' \
                                           '?count=30&' \
                                           'active_status=all&' \
                                           'ad_type=all&' \
                                           'countries[0]=US&' \
                                           'impression_search_field=has_impressions_lifetime&' \
                                           f'view_all_page_id={self.page_id}&' \
                                           'sort_data[direction]=desc&' \
                                           'sort_data[mode]=relevancy_monthly_grouped'
        if self.collation_token and self.forward_cursor:
            url += f"&forward_cursor={self.forward_cursor}"
            url += f"&collation_token={self.collation_token}"
        yield scrapy.FormRequest(url, callback=self.parse_page, formdata={'__a': "1"})

    def parse_page(self, response):
        res = response.body.decode("utf-8").replace("for (;;);", "")
        res = json.loads(res)
        insert_data(res)


def insert_data(data):
    connection = MongoDBConnectionClass()
    processDb = connection.connectToMongo()
    adCol = processDb.fb_ads

    payload = data["payload"]
    data_to_insert = payload["results"]

    data_to_insert = [item[0] for item in data_to_insert]
    adCol.insert(data_to_insert)
    print("record inserted")

    if not payload["isResultComplete"]:
        process.crawl(MySpider, page_id=page_id, collation_token=payload["collationToken"], forward_cursor=payload["forwardCursor"])


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
page_id = 335963307560
process.crawl(MySpider, page_id=page_id, collation_token="", forward_cursor="")
process.start()
