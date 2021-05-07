import pprint
import datetime

import pymongo


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
        for website in websites:
            print(website['site'])
    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)


if __name__ == "__main__":
    print(read_from_websites())
