import datetime

import pymongo
import scrapy
from scrapy.crawler import CrawlerProcess
import tldextract
from scrapy.linkextractors import LinkExtractor


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
        # try:
        #     conn = pymongo.MongoClient('localhost', 27017)
        #     db = conn.tsearch
        #     collection = db.crawl_data
        #     potential_match = collection.find_one(
        #         {
        #             'site': response.request.url
        #         }
        #     )
        #     conn.close()
        # except pymongo.errors.ConnectionFailure as e:
        #     print("Could not connect to MongoDB: %s" % e)
        crawl_date = datetime.datetime.now()
        # *charset: todo - parse web page using the encoding specified
        # meta charset=""
        # *title:
        # title
        title = response.css('title')[0].css('::text').get()
        # meta property="og:title" content=""
        # *image:
        # meta content="" itemprop="image"
        # meta property="og:image" content=""
        image_url = response.css('meta[itemprop="image"]::attr(content)').extract()
        if not image_url:
            image_url = response.css('meta[property="og:image"]::attr(content)').extract()
        # *description
        # meta name="description" content=""
        # meta name="Description" content=""
        # meta property="og:description" content=""
        site_description = response.css('meta[name="description"]::attr(content)').extract()
        if not site_description:
            site_description = response.css('meta[name="Description"]::attr(content)').extract()
        if not site_description:
            site_description = response.css('meta[property="og:description"]::attr(content)').extract()
        # *keywords:
        # meta name="keywords" content=""
        site_keywords = response.css('meta[name="keywords"]::attr(content)').extract()
        # *theme (color):
        # meta name="theme-color" content=""
        site_theme = response.css('meta[name="theme-color"]::attr(content)').extract()
        # *url (to access site):
        # meta property="og:url" content=""
        site_url_meta = response.css('meta[property="og:url"]::attr(content)').extract()
        # *locale:
        # meta property="og:locale" content=""
        site_locale = response.css('meta[property="og:locale"]::attr(content)').extract()
        # *twitter:
        # meta name="twitter:site" content=""
        # meta name="twitter:creator" content=""
        twitter = response.css('meta[name="twitter:site"]::attr(content)').extract()
        if not twitter:
            twitter = response.css('meta[name="twitter:creator"]::attr(content)').extract()
        # *twitter image source
        # meta name="twitter:image:src" content=""
        twitter_image_source = response.css('meta[name="twitter:image:src"]::attr(content)').extract()
        # *facebook integration app id:
        # meta property="fb:app_id" content=""
        facebook_integration_app_id = response.css('meta[property="fb:app_id"]::attr(content)').extract()
        # *apple mobile web app title
        # meta name="apple-mobile-web-app-title" content=""
        # meta name="application-name" content=""
        apple_mobile_web_app_title = response.css('meta[name="apple-mobile-web-app-title"]::attr(content)').extract()
        if not apple_mobile_web_app_title:
            apple_mobile_web_app_title = response.css('meta[name="application-name"]::attr(content)').extract()
        try:
            conn = pymongo.MongoClient('localhost', 27017)
            db = conn.tsearch
            # *links to other pages:
            # a href=""
            links = []
            for link in response.css('a::attr(href)'):
                href_attribute = link.extract()
                tld_ext = tldextract.extract(href_attribute)
                collection = db.websites
                check_link = collection.find_one(
                    {
                        'site': tld_ext.domain + '.' + tld_ext.suffix
                    }
                )
                if check_link:
                    # check that the domain is anything but this
                    if tld_ext.domain + '.' + tld_ext.suffix != tldextract.extract(response.request.url).domain + '.' + tldextract.extract(response.request.url).suffix:
                        links.append(href_attribute)
                else:
                    if tld_ext.domain:
                        collection = db.external_link_discovery
                        doc_structure = {
                            'crawl_date': crawl_date,
                            'referrer': response.request.url,
                            'external_link': href_attribute
                        }
                        try:
                            collection.insert_one(doc_structure)
                        except Exception as e:
                            print("An exception occurred ::", e)
            collection = db.crawl_data
            original_domain = tldextract.extract(response.request.url).domain + '.' + tldextract.extract(response.request.url).suffix
            doc_structure = {
                'crawl_date': crawl_date,
                'site': response.request.url,
                'original_domain': original_domain,
                'title': title,
                'image_url': image_url,
                'site_description': site_description,
                'site_keywords': site_keywords,
                'site_theme': site_theme,
                'site_url_meta': site_url_meta,
                'site_locale': site_locale,
                'twitter': twitter,
                'twitter_image_source': twitter_image_source,
                'facebook_integration_app_id': facebook_integration_app_id,
                'apple_mobile_web_app_title': apple_mobile_web_app_title,
                'links': links
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
