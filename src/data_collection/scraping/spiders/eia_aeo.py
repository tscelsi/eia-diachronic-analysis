"""Retrieve the PDF files associated with the EIA Annual energy outlook over 
the past years.

Running spider: scrapy crawl --set=ROBOTSTXT_OBEY='False' eia-annual-energy-outlook
"""

import scrapy
from bs4 import BeautifulSoup
import re, io, os
from scrapy.shell import inspect_response

class AnnualEnergyOutlook(scrapy.Spider):
    name="eia-annual-energy-outlook"

    start_urls = [
        "https://www.eia.gov/outlooks/aeo/archive.php"
    ]

    def __init__(self, save_path):
        self.aeo_pdf_save_path = save_path

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
        tr_arr = soup.find("table").find_all("tr")
        # remove first two rows
        tr_arr = tr_arr[2:]
        page_links = []
        for row in tr_arr:
            pdf_link = row.find_all("td")[1].find("a").get("href")
            page_links.append(stem + pdf_link)
        for link in page_links:
            yield scrapy.Request(link, self.get_pdf)
        
    def get_pdf(self, response):
        """Saves pdf to 

        Args:
            response (scrapy.http.repsonse.Response): response that holds the pdf document
        """
        file_name = re.match(r".*/(.*\.pdf)", response.url).group(1)
        with open(os.path.join(self.aeo_pdf_save_path, file_name), "wb") as fp:
            fp.write(response.body)

        
    