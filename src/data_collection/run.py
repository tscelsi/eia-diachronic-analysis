"""This file contains a script to commence the collection of all PDF Roadmap files

When run as __main__, this file scrapes the EIA and IRENA websites to retrieve relevant documents 
for conversion from pdf to text. Here the pdfs are collected, and saved into respective folders as outlined by the *_SAVE_PATH
default variables, or in the configuration file "config.json".

To run: python3 run.py
"""

import os
import sys
import json
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


settings_file_path = 'data_collection.scraping.settings' # The path seen from root, ie. from main.py
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
FILE_SAVE_DIR = "../static/corpora/data/"

def init():
    """Load config and perform appropriate checks

    Returns:
        dict: arguments to be used by scrapers
    """
    if not os.path.isdir(FILE_SAVE_DIR):
        os.mkdir(FILE_SAVE_DIR)
    AEO_SAVE_PATH = os.path.join(FILE_SAVE_DIR, "aeo")
    IEO_SAVE_PATH = os.path.join(FILE_SAVE_DIR, "ieo")
    if not os.path.exists(AEO_SAVE_PATH):
        os.mkdir(AEO_SAVE_PATH)
    if not os.path.exists(IEO_SAVE_PATH):
        os.mkdir(IEO_SAVE_PATH)
    # check if directories exist, if not, create them.
    if not os.path.isdir(AEO_SAVE_PATH):
        print(f"{AEO_SAVE_PATH} doesn't exist. You will need to create the appropriate directories first:\n{AEO_SAVE_PATH}")
        sys.exit(0)
    if not os.path.isdir(IEO_SAVE_PATH):
        print(f"{IEO_SAVE_PATH} doesn't exist. You will need to create the appropriate directories first:\n{IEO_SAVE_PATH}")
        sys.exit(0)
    print(f"AEO SAVE PATH: {AEO_SAVE_PATH}")
    print(f"IEO SAVE PATH: {IEO_SAVE_PATH}")
    return {
        "eia-annual-energy-outlook": AEO_SAVE_PATH,
        "eia-international-energy-outlook": IEO_SAVE_PATH,
    }


def run():
    args = init()
    process = CrawlerProcess(get_project_settings())
    process.crawl("eia-annual-energy-outlook",
                  save_path=args["eia-annual-energy-outlook"])
    process.crawl("eia-international-energy-outlook",
                  save_path=args["eia-international-energy-outlook"])
    process.start()


def run_aeo():
    args = init()
    process = CrawlerProcess(get_project_settings())
    process.crawl("eia-annual-energy-outlook",
                  save_path=args["eia-annual-energy-outlook"])
    process.start()

if __name__ == "__main__":
    run()