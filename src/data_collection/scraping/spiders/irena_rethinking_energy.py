"""Retrieve the PDF files associated with the IRENA organisation over 
the past years.

Running spider: scrapy crawl --set=ROBOTSTXT_OBEY='False' irena_rethinking_energy
"""

import scrapy
from bs4 import BeautifulSoup
import re, io, os, sys
import importlib


class IRENARethinkingEnergy(scrapy.Spider):
    name="irena_rethinking_energy"

    start_urls = [
        "https://www.irena.org/publications/Our-Collections#remap"
    ]

    def __init__(self, save_path):
        self.irena_rethinking_energy_save_path = save_path

    def parse(self, response):
        """This function is called on all urls in start_urls. Specifically,
        it returns the html to then be scraped to find the urls to all pdfs
        from eia short term energy outlook page.

        Args:
            response (scrapy.http.repsonse.html.HtmlResponse): respone class to be made into soup

        Yields:
            scrapy.Request: scrapy Request object that holds the url to a pdf file to be scraped and saved.
        """
        soup = BeautifulSoup(response.text)
        stem = "https://www.irena.org"
        rethinking_energy = {item['href'] for item in soup.find(id="rethinking_energy").next_sibling.next_sibling.find(class_="owl-carousel").find_all("a")}
        for link in rethinking_energy:
            yield scrapy.Request(stem + link, self.get_pdf_dl_link)
        
    def get_pdf_dl_link(self, response):
        stem = "https://www.irena.org"
        soup = BeautifulSoup(response.text)
        suffix = soup.find(class_="publication-download").find("a")['href']
        yield scrapy.Request(stem + suffix, self.get_pdf)

    def get_pdf(self, response):
        """Saves pdf to 

        Args:
            response (scrapy.http.repsonse.Response): response that holds the pdf document
        """
        file_name = re.match(r".*/(.*\.pdf)", response.url).group(1)
        with open(os.path.join(self.irena_rethinking_energy_save_path, file_name), "wb") as fp:
            fp.write(response.body)

        
    