"""Retrieve the PDF files associated with the EIA Short Term Energy Outlook over 
the past years.

Running spider: scrapy crawl --set=ROBOTSTXT_OBEY='False' eia-short-term-energy-outlook
"""

import scrapy
from bs4 import BeautifulSoup
import re, io, os, sys
sys.path.append(os.path.join(os.environ['ROADMAP_SCRAPER'], "helper"))
import pdf


class ShortTermEnergyOutlook(scrapy.Spider):
    name="eia-short-term-energy-outlook"

    start_urls = [
        "https://www.eia.gov/outlooks/steo/outlook.php"
    ]

    def __init__(self, save_path):
        self.steo_pdf_save_path = save_path

    def parse(self, response):
        """This function is called on all urls in start_urls. Specifically,
        it returns the html to then be scraped to find the urls to all pdfs
        from eia short term energy outlook page.

        Args:
            response (scrapy.http.repsonse.html.HtmlResponse): respone class to be made into soup

        Yields:
            scrapy.Request: scrapy Request object that holds the url to a pdf file to be scraped and saved.
        """
        stem = "https://www.eia.gov/outlooks/steo/"
        soup = BeautifulSoup(response.text)
        tbody_arr = soup.find_all("tbody")
        page_links = []
        for tbody in tbody_arr:
            tbody_links = [anchor.get("href") for anchor in filter(pdf.is_pdf, tbody.find_all("a"))]
            page_links.extend(tbody_links)
        for link in page_links:
            yield scrapy.Request(stem + link, self.get_pdf)
        
    def get_pdf(self, response):
        """Saves pdf to 

        Args:
            response (scrapy.http.repsonse.Response): response that holds the pdf document
        """
        file_name = re.match(r".*/(.*\.pdf)", response.url).group(1)
        with open(os.path.join(self.steo_pdf_save_path, file_name), "wb") as fp:
            fp.write(response.body)

        
    