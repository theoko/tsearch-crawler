import os
import csv
import scrapy
import urllib.request
import zipfile

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


def parse_top_one_million_csv(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            print(', '.join(row))
            print(row[1])  # url
            break


if __name__ == "__main__":
    create_csv_dir()
    download_url(one_million_sites, "csv/sites.csv.zip")
    unzip_file("csv/sites.csv.zip")
    parse_top_one_million_csv("csv/top-1m.csv")
