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
import argparse

# DEFAULT_EIA_AEO_SAVE_PATH = os.path.join(
#     os.environ['ROADMAP_DATA'], "eia", "files", "aeo")
# DEFAULT_EIA_IEO_SAVE_PATH = os.path.join(
#     os.environ['ROADMAP_DATA'], "eia", "files", "ieo")
# DEFAULT_EIA_STEO_SAVE_PATH = os.path.join(
#     os.environ['ROADMAP_DATA'], "eia", "files", "steo")
# DEFAULT_ARENA_SAVE_PATH = os.path.join(
#     os.environ['ROADMAP_DATA'], "arena", "files")
# DEFAULT_IRENA_TECH_BRIEFS_SAVE_PATH = os.path.join(
#     os.environ['ROADMAP_DATA'], "irena", "files", "tech_briefs")

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")


def init(save_dir):
    """Load config and perform appropriate checks

    Returns:
        dict: arguments to be used by scrapers
    """
    if not os.path.isdir(save_dir):
        os.mkdir(save_dir)
    AEO_SAVE_PATH = os.path.join(save_dir, "aeo")
    IEO_SAVE_PATH = os.path.join(save_dir, "ieo")
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


def run(save_dir):
    args = init(save_dir)
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