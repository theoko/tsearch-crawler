import pprint
import datetime

import pymongo
import scrapy
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner, CrawlerProcess
from scrapy.utils.project import get_project_settings


def read_from_websites():
    job_date = datetime.datetime.now()
    try:
        conn = pymongo.MongoClient('localhost', 27017)
        db = conn.tsearch
        collection = db.job_dates
        # pprint.pprint(collection.find_one(
        #     sort=[('job_date', pymongo.DESCENDING)]
        # ))
        last_date = collection.find_one(
            sort=[('job_date', pymongo.DESCENDING)]
        )
        last_date = last_date['job_date']
        collection = db.websites
        websites = collection.find(
            {
                "job_date": last_date
            }
        )
        url_list = []
        for website in websites:
            url_list.append(website['site'])
        collection = db.job_dates
        doc_structure = {
            'job_date': job_date,
            'job_file': 'job2.py'
        }
        try:
            collection.insert_one(doc_structure)
        except Exception as e:
            print("An exception occurred ::", e)
        conn.close()
        return url_list
    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)


class EngineSpider(scrapy.Spider):
    name = "tsearchspider"

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.start_urls = kwargs.pop('url_list', [])
        super(EngineSpider, *args, **kwargs)

    def parse(self, response):
        crawl_date = datetime.datetime.now()
        try:
            conn = pymongo.MongoClient('localhost', 27017)
            db = conn.tsearch
            collection = db.crawl_data
            doc_structure = {
                'crawl_date': crawl_date,
                'response': response
            }
            try:
                collection.insert_one(doc_structure)
            except Exception as e:
                print("An exception occurred ::", e)
            conn.close()
        except pymongo.errors.ConnectionFailure as e:
            print("Could not connect to MongoDB: %s" % e)


if __name__ == "__main__":
    website_urls = read_from_websites()
    process = CrawlerProcess()
    process.crawl(EngineSpider)
    process.start()
