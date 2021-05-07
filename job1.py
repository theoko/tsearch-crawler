import datetime
import os
import urllib.request
import zipfile

import pymongo

import csv

one_million_sites = "https://s3.amazonaws.com/alexa-static/top-1m.csv.zip"


def create_csv_dir():
    if not os.path.exists("csv"):
        os.mkdir("csv")


def download_url(url, save_path):
    with urllib.request.urlopen(url) as dl_file:
        with open(save_path, 'wb') as out_file:
            out_file.write(dl_file.read())


def unzip_file(file_path):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall("./csv")
        zip_ref.close()


def parse_top_one_million_csv(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        sites = []
        for row in reader:
            # print(', '.join(row))
            sites.append(row[1])  # url
            # break
        csvfile.close()
        return sites


def insert_into_mongo(sites):
    job_date = datetime.datetime.now()
    try:
        conn = pymongo.MongoClient('localhost', 27017)
        db = conn.tsearch
        collection = db.websites
        for site in sites:
            doc_structure = {
                'job_date': job_date,
                'site': site
            }
            try:
                collection.insert_one(doc_structure)
            except Exception as e:
                print("An exception occurred ::", e)
        collection = db.job_dates
        try:
            doc_structure = {
                'job_date': job_date,
                'total_websites': len(sites)
            }
            collection.insert_one(doc_structure)
        except Exception as e:
            print("An exception occurred ::", e)
        conn.close()
    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)


if __name__ == "__main__":
    create_csv_dir()
    download_url(one_million_sites, "csv/sites.csv.zip")
    unzip_file("csv/sites.csv.zip")
    websites = parse_top_one_million_csv("csv/top-1m.csv")
    insert_into_mongo(websites)
