"""Retrieve the PDF files associated with potential ARENA Roadmaps

Running spider: scrapy crawl --set=ROBOTSTXT_OBEY='False' arena
"""

import scrapy
import re, os


class Arena(scrapy.Spider):
    name="arena"

    def __init__(self, save_path, **kwargs):
        self.arena_pdf_save_path = save_path
        self.start_url_stem = "https://arena.gov.au/wp-json/wp/v2/knowledge-bank?_embed=true&_envelope=true&orderby=date&all_posts=true"
        self.create_arena_url(**kwargs)

    def create_arena_url(self, **kwargs):
        """This function takes configuration items for the url that will decide what documents are returned in the csv file.
        """
        filter_technology = kwargs.pop("filter_technology", "")
        filter_item_type = kwargs.pop("filter_item_type", "Reports")
        filter_year = kwargs.pop("filter_year", "")
        filter_status = "current"
        filter_keywords = kwargs.pop("filter_keywords", "")
        self.start_url_whole = f"{self.start_url_stem}&kb_filter_technology={filter_technology}&kb_item_type={filter_item_type}&kb_filter_state=&kb_filter_year={filter_year}&kb_filter_status=current={filter_status}&kb_filter_search={filter_keywords}"
        self.logger.info(f"url: {self.start_url_whole}")

    def start_requests(self):
        return [scrapy.Request(self.start_url_whole, callback=self.get_documents)]

    def get_documents(self, response):
        documents = response.json()['body']
        for doc in documents:
            metadata = self.get_metadata(doc)
            link = doc['acf']['download']['url']
            yield scrapy.Request(link, callback=self.get_pdf, cb_kwargs=metadata)

    def get_metadata(self, document):
        """This function will be able to extract various informative metadata from each documents json representation.
        e.g. author, year of publication etc.

        Args:
            document (dict): document containing metadata to be extracted.
        """
        return {}

    def get_pdf(self, response, **kwargs):
        """Saves pdf to 

        Args:
            response (scrapy.http.repsonse.Response): response that holds the pdf document
        """
        file_name = re.match(r".*/(.*\.pdf)", response.url).group(1)
        with open(os.path.join(self.arena_pdf_save_path, file_name), "wb") as fp:
            fp.write(response.body)

        
    