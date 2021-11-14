"""Retrieve the PDF files associated with the EIA International energy outlook over 
the past years.

Running spider: scrapy crawl --set=ROBOTSTXT_OBEY='False' eia-international-energy-outlook
"""

import scrapy
from bs4 import BeautifulSoup
import re, io, os, sys
import importlib


class InternationalEnergyOutlook(scrapy.Spider):
    name="eia-international-energy-outlook"

    start_urls = [
        "https://www.eia.gov/outlooks/ieo/ieoarchive.php"
    ]

    def __init__(self, save_path):
        self.ieo_pdf_save_path = save_path

    def parse(self, response):
        """This function is called on all urls in start_urls. Specifically,
        it returns the html to then be scraped to find the urls to all pdfs
        from eia short term energy outlook page.

        Args:
            response (scrapy.http.repsonse.html.HtmlResponse): respone class to be made into soup

        Yields:
            scrapy.Request: scrapy Request object that holds the url to a pdf file to be scraped and saved.
        """
        stem = "https://www.eia.gov"
        soup = BeautifulSoup(response.text)
        anchors = soup.find_all("a", class_="ico_pdf")
        # we initialise this with the most recent ieo because they are not in archive
        page_links = [
            "https://www.eia.gov/outlooks/ieo/pdf/executive_summary.pdf",
            "https://www.eia.gov/outlooks/ieo/africa/pdf/africa_detailed.pdf",
            "https://www.eia.gov/outlooks/ieo/china/pdf/china_detailed.pdf",
            "https://www.eia.gov/outlooks/ieo/india/pdf/india_detailed.pdf",
            "https://www.eia.gov/outlooks/archive/ieo19/pdf/ieo2019.pdf",
            "https://www.eia.gov/outlooks/ieo/pdf/IEO2020_IIF_Asia.pdf",
            "https://www.eia.gov/outlooks/ieo/pdf/IEO2020_IIF_India.pdf",
            "https://www.eia.gov/outlooks/ieo/pdf/IEO2020_IIF_Africa.pdf"
        ]
        for a in anchors:
            page_links.append(stem + a.get("href"))
        for link in page_links:
            yield scrapy.Request(link, self.get_pdf)
        
    def get_pdf(self, response):
        """Saves pdf to 

        Args:
            response (scrapy.http.repsonse.Response): response that holds the pdf document
        """
        file_name = re.match(r".*/(.*\.pdf)", response.url).group(1)
        with open(os.path.join(self.ieo_pdf_save_path, file_name), "wb") as fp:
            fp.write(response.body)

        
    