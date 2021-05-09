import datetime

import pymongo
import scrapy
from scrapy.crawler import CrawlerProcess


def read_from_websites():
    job_date = datetime.datetime.now()
    try:
        conn = pymongo.MongoClient('localhost', 27017)
        db = conn.tsearch
        collection = db.job_dates
        last_date = collection.find_one(
            {
                'job_file': 'job1.py'  # filter for job1.py which is the job that collects the set of websites
            },
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
            url_list.append('https://' + website['site'])
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

    def __init__(self, *a, **kw):
        self.start_urls = kw.get('start_url')
        super(EngineSpider, self).__init__(*a, **kw)

    def parse(self, response):
        crawl_date = datetime.datetime.now()
        # *charset:
        # meta charset=""
        # *title:
        # title
        # meta property="og:title" content=""
        # *image:
        # meta content="" itemprop="image"
        # meta property="og:image" content=""
        # *description
        # meta name="description" content=""
        # meta name="Description" content=""
        # meta property="og:description" content=""
        # *keywords:
        # meta name="keywords" content=""
        # *theme (color):
        # meta name="theme-color" content=""
        # *url (to access site):
        # meta property="og:url" content=""
        # locale:
        # meta property="og:locale" content=""
        # *facebook integration:
        # meta property="fb:app_id" content=""
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
    process.crawl(EngineSpider, start_url=website_urls)
    process.start()
