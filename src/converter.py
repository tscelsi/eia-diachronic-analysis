"""Code for conversion of pdf files to text

This file converts a pdf file to it's structured json representation. The RoadmapPDFConverter class
is the base class which contains the logic for conversion using pdfminer.six. Each different document
category (e.g. steo, ieo, tech_briefs) has a different subclass implementation of RoadmapPDFConverter 
mainly to pass in the correct directories that contain the pdfs and to implement any organisation-specific filtering.

To run: python3 converter.py
"""

from pdfminer.high_level import extract_text, extract_pages, extract_text_to_fp
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams, LTTextContainer, LTChar
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from collections import Counter
from io import StringIO
from multiprocessing import Process, Pool
import os
import json
import sys
import re
from bs4 import BeautifulSoup
from utils.pdf import CustomRoadMapConverter

NUM_CPU = os.cpu_count() - 1 if os.cpu_count() > 1 else 1

class RoadmapPDFConverter:
    """
    This class is responsible for taking in a config file for a set/single pdf and converting that pdf to text.

    """
    dir_path = ""
    config_path = os.path.join(os.path.dirname(__file__), "default_scraping_config.json")
    laparams = None

    def __init__(self, custom_filter=None, perdoc_config=False, **kwargs):
        self.custom_filter = None
        if custom_filter:
            self.custom_filter = custom_filter
        self.perdoc_config = perdoc_config
        self._load_config()
        self._init_config(**kwargs)
        return

    def _init_doc_config(self, **kwargs):
        self.filename = kwargs.pop("filename")
        self.config_path = kwargs.pop("config")
        self.output_path = kwargs.pop("outfile")
        if not self.filename or not self.output_path:
            print("need filename and output path in kwargs")
            sys.exit(0)
        if self.config_path:
            with open(self.config_path, "r") as fp:
                config = json.load(fp)
        else:
            print(
                f"no config file found for {self._get_file_name(self.filename)} falling back onto defaults.")
            self._init_config(**self.config)
            return
        if self.perdoc_config:
            self._init_config(**{**self.config, **config})

    def _get_doc_config(self, default_config, perdoc_config=True, **kwargs):
        filename = kwargs.pop("filename")
        config_path = kwargs.pop("config")
        output_path = kwargs.pop("outfile")
        if not filename or not output_path:
            print("need filename and output path in kwargs")
            return None
        if config_path:
            with open(config_path, "r") as fp:
                config = json.load(fp)
        else:
            print(
                f"no config file found at {config_path} falling back onto defaults.")
            return self._init_config(**{**default_config, "filename": filename, "output_path": output_path})
        if perdoc_config:
            return self._init_config(**{**default_config, **config, "filename": filename, "output_path": output_path})
        else:
            return self._init_config(**{**default_config, "filename": filename, "output_path": output_path})

    def _load_config(self):
        with open(self.config_path, "r") as fp:
            self.config = json.load(fp)

    def _init_config(self, exclusions_exact=[], exclusions_page=[], inclusions_font=[], table_of_contents=[], laparams=None, to_filter=False, **kwargs):
        """In the case of per doc configs, the class configuration parameters need to be updated for each individual pdf.
        This will be called from the init doc config function.
        """
        if laparams:
            self.laparams = LAParams(**laparams)
        else:
            self.laparams = LAParams()
        self.to_filter = to_filter
        self.table_of_contents = table_of_contents
        self.exclusions_exact = exclusions_exact
        self.exclusions_page = exclusions_page
        self.inclusions_font = inclusions_font
        return {**kwargs, "laparams": self.laparams, "to_filter": self.to_filter, "table_of_contents": self.table_of_contents, "exclusions_exact": self.exclusions_exact, "exclusions_page": self.exclusions_page, "inclusions_font": self.inclusions_font}

    def parse_multiple_to_json(self):
        paths = self._get_pdf_and_config_paths()
        for item in paths:
            self._parse_pdf_to_json(**item)

    def mp_parse_multiple_to_json(self):
        paths = self._get_pdf_and_config_paths()
        pool = Pool(processes=NUM_CPU/2 if NUM_CPU != 1 else 1)
        a = [pool.apply_async(self._mp_parse_pdf_to_json, kwds=path)
             for path in paths]
        for future in a:
            res = future.get()
            if res[1] == 0:
                print(f"convert failed somewhere for {res[0]}")

    def _gen_pdf(self):
        dir_files = os.listdir(self.dir_path)
        for filename in dir_files:
            if self._is_pdf(filename):
                yield os.path.join(self.dir_path, filename)

    def _get_pdf_and_config_paths(self):
        """Retrieves all the pdf files from the directory path specified in the class variable self.dir_path.
        If there is a config file specified alongside the pdf, it will retrieve this as well. else config will be None.

        Yields:
            dict: file paths for various relevant files
        """
        dir_files = os.listdir(self.dir_path)
        for filename in dir_files:
            if self._is_pdf(filename):
                fn = self._get_file_name(filename)
                if self.perdoc_config:
                    config = os.path.join(self.dir_path, fn + ".conf") if os.path.isfile(
                        os.path.join(self.dir_path, fn + ".conf")) else None
                else:
                    config = self.config_path
                yield {"filename": os.path.join(self.dir_path, filename), "config": config, "outfile": os.path.join(self.dir_path, fn + f"_structured.json")}

    def _parse_pdf_to_json(self, **kwargs):
        self._init_doc_config(**kwargs)
        chunks = self._parse(self.filename)
        chunks['doc'] = self._merge_chunks(chunks['doc'])
        with open(self.output_path, "w") as wfp:
            json.dump(chunks, wfp)

    def _mp_parse_pdf_to_json(self, **kwargs):
        filename = kwargs.get("filename", None)
        config = self._get_doc_config(
            self.config, self.perdoc_config, **kwargs)
        if not config:
            return (None, 0)
        chunks = self._mp_parse(**config)
        chunks['doc'] = self._merge_chunks(chunks['doc'])
        output_path = config.get("output_path", None)
        if output_path:
            with open(output_path, "w") as wfp:
                json.dump(chunks, wfp)
                return (filename, 1)
        else:
            fn = config.get("filename", "unknown")
            print(f"output_path not found in config for {fn}")
            return (filename, 0)

    def _merge_chunks(self, chunks):
        merged_chunks = []
        acc_chunk = {}
        i = 0
        while i < len(chunks):
            if acc_chunk == {}:
                acc_chunk = chunks[i]
            else:
                if acc_chunk['size'] == chunks[i]['size']:
                    acc_chunk['text'] = ' '.join(
                        [acc_chunk['text'], chunks[i]['text']])
                else:
                    merged_chunks.append(acc_chunk)
                    acc_chunk = chunks[i]
            i += 1
        merged_chunks.append(acc_chunk)
        return merged_chunks

    def _is_pdf(self, file_name):
        file_name = re.match(r"(.*)\.pdf$", file_name)
        if file_name:
            return True
        else:
            return False

    def _get_file_name(self, file):
        """Retrieves the suffix of a file path without the extension of .pdf, .json etc. to be used in other file
        type naming

        Args:
            file (string): fully qualified path to a file
        """
        file_name = re.match(r"(.*\\)?(.*)\.[a-z]+$", file).group(2)
        return file_name

    def _mp_parse(self, **kwargs):
        fn = kwargs.get("filename", None)
        table_of_contents = kwargs.get("table_of_contents", None)
        print(f"converting {fn}...")
        # extract table of contents
        toc = []
        # extract rest
        line_gen = self._mp_create_text_line_generator(**kwargs)
        chunks = []
        curr_chunk = {"text": "", "page": None, "size": None}
        for line in line_gen:
            if curr_chunk['text'] == "":
                curr_chunk = line
            elif line['size'] != curr_chunk['size']:
                preproc_chunk = self._preprocess(curr_chunk)
                if preproc_chunk['text'] != "":
                    chunks.append(preproc_chunk)
                if curr_chunk['page'] in table_of_contents:
                    toc.append(curr_chunk)
                curr_chunk = line
            elif line['page'] != curr_chunk['page']:
                # if the page changes, but not necessarily different sizes
                if curr_chunk['page'] in table_of_contents:
                    toc.append(curr_chunk)
                curr_chunk["text"] = ' '.join(
                    [curr_chunk['text'], line['text']])
            else:
                curr_chunk["text"] = ' '.join(
                    [curr_chunk['text'], line['text']])
        if curr_chunk['page'] in table_of_contents:
            toc.append(curr_chunk)
        else:
            # chunks.append(curr_chunk)
            preproc_chunk = self._preprocess(curr_chunk)
            if preproc_chunk['text'] != "":
                chunks.append(preproc_chunk)
        return {"doc": chunks, "table_of_contents": toc}

    def _parse(self, pdf_file):
        print(f"converting {self.filename}...")
        # extract table of contents
        toc = []
        # extract rest
        line_gen = self._create_text_line_generator(pdf_file)
        chunks = []
        curr_chunk = {"text": "", "page": None, "size": None}
        for line in line_gen:
            if curr_chunk['text'] == "":
                curr_chunk = line
            elif line['size'] != curr_chunk['size']:
                preproc_chunk = self._preprocess(curr_chunk)
                if preproc_chunk['text'] != "":
                    chunks.append(preproc_chunk)
                if curr_chunk['page'] in self.table_of_contents:
                    toc.append(curr_chunk)
                curr_chunk = line
            elif line['page'] != curr_chunk['page']:
                # if the page changes, but not necessarily different sizes
                if curr_chunk['page'] in self.table_of_contents:
                    toc.append(curr_chunk)
                curr_chunk["text"] = ' '.join(
                    [curr_chunk['text'], line['text']])
            else:
                curr_chunk["text"] = ' '.join(
                    [curr_chunk['text'], line['text']])
        if curr_chunk['page'] in self.table_of_contents:
            toc.append(curr_chunk)
        else:
            # chunks.append(curr_chunk)
            preproc_chunk = self._preprocess(curr_chunk)
            if preproc_chunk['text'] != "":
                chunks.append(preproc_chunk)
        return {"doc": chunks, "table_of_contents": toc}

    def _mp_line_filter(self, lines, **kwargs):
        exclusions_page = kwargs.get("exclusions_page", None)
        exclusions_exact = kwargs.get("exclusions_exact", None)
        inclusions_font = kwargs.get("inclusions_font", None)
        filtered_lines = []
        for line in lines:
            exclude = False
            # filter page number
            if line['page'] in exclusions_page:
                continue
            if -1 in exclusions_page:
                ind = exclusions_page.index(-1)
                if ind == 0 or ind == 1:
                    print("Can't have -1 as first or second element of the page exclusions, \
                        the -1 must follow the min, max page you want to exclude.\
                            e.g. [111, 128, -1] excludes pages between 111 and 128.")
                    sys.exit(0)
                from_ = exclusions_page[ind - 2]
                to_ = exclusions_page[ind - 1]
                if line['page'] >= from_ and line['page'] <= to_:
                    continue
            # filter exact string line matches
            for regex in exclusions_exact:
                if re.match(regex, line['text']):
                    exclude = True
                    break
            if exclude:
                continue
            # filter font size
            if line['size'] not in inclusions_font:
                # any font size over 18 regardless of included or not, will be
                # included as it is most definitely a heading.
                if line['size'] < 18:
                    continue

            filtered_lines.append(line)
        return filtered_lines

    def _line_filter(self, lines):
        """line filtering based on configuration items, removes unwanted lines, string matches etc.

        Args:
            soup ([type]): [description]
        """
        filtered_lines = []
        for line in lines:
            exclude = False
            # filter page number
            if line['page'] in self.exclusions_page:
                continue
            if -1 in self.exclusions_page:
                ind = self.exclusions_page.index(-1)
                if ind == 0 or ind == 1:
                    print("Can't have -1 as first or second element of the page exclusions, \
                        the -1 must follow the min, max page you want to exclude.\
                            e.g. [111, 128, -1] excludes pages between 111 and 128.")
                    sys.exit(0)
                from_ = self.exclusions_page[ind - 2]
                to_ = self.exclusions_page[ind - 1]
                if line['page'] >= from_ and line['page'] <= to_:
                    continue
            # filter exact string line matches
            for regex in self.exclusions_exact:
                if re.match(regex, line['text']):
                    exclude = True
                    break
            if exclude:
                continue
            # filter font size
            if line['size'] not in self.inclusions_font:
                # any font size over 18 regardless of included or not, will be
                # included as it is most definitely a heading.
                if line['size'] < 18:
                    continue

            filtered_lines.append(line)
        return filtered_lines

    def _preprocess(self, line):
        """final preprocessing applied to paragraph format of text. This is mainly used to merge words that
        are split over a new line (e.g. effici-\nency --> efficiency) and to clean whitespace.

        Args:
            line (dict): json output format (e.g. {"page": 7, "size": 14, "text": "i am a camel"})

        Returns:
            dict: json output format
        """
        if self.custom_filter:
            line['text'] = self.custom_filter(line['text'])
        new_line = re.sub(r"\(cid:\d+\)", " ", line['text'])
        new_line = re.sub(r"\-\n\s+", " ", new_line)
        # remove most prevalent non-ascii chars
        new_line = re.sub(r"\xa0", " ", new_line)
        # sub single \n instances in line with space
        new_line = re.sub(r"\n{1}", " ", new_line)
        # substitue spaces of more than one with a single space.
        new_line = re.sub(r"\s\s+", " ", new_line)
        new_line = new_line.strip()
        line['text'] = new_line
        return line

    def _mp_create_text_line_generator(self, **kwargs):
        """Creates a text specific line generator that returns a line of text
        in the form {size: <size>, text: <text>}. Supports multiprocessing (hopefully)

        Args:
            pdf_file (String): the path to the pdf file to be generated
        """
        laparams = kwargs.get("laparams", None)
        to_filter = kwargs.get("to_filter", None)
        filename = kwargs.get("filename", None)
        output_string = StringIO()
        with open(filename, 'rb') as in_file:
            parser = PDFParser(in_file)
            doc = PDFDocument(parser)
            rsrcmgr = PDFResourceManager()
            device = CustomRoadMapConverter(rsrcmgr, laparams=laparams)
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            for page in PDFPage.create_pages(doc):
                interpreter.process_page(page)
        if to_filter:
            lines = self._mp_line_filter(device.lines, **kwargs)
        else:
            lines = device.lines
        for line in lines:
            yield line

    def _create_text_line_generator(self, pdf_file):
        """Creates a text specific line generator that returns a line of text
        in the form {size: <size>, text: <text>}.

        Args:
            pdf_file (String): the path to the pdf file to be generated
        """
        output_string = StringIO()
        with open(pdf_file, 'rb') as in_file:
            parser = PDFParser(in_file)
            doc = PDFDocument(parser)
            rsrcmgr = PDFResourceManager()
            print(self.laparams)
            device = CustomRoadMapConverter(rsrcmgr, laparams=self.laparams)
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            for page in PDFPage.create_pages(doc):
                interpreter.process_page(page)
        if self.to_filter:
            lines = self._line_filter(device.lines)
        else:
            lines = device.lines
        for line in lines:
            yield line

    def get_metadata(self):
        pass


def parse_organisation():
    c = EIAIEOConverter()
    c.mp_parse_multiple_to_json()


def parse_single():
    file_path = os.path.join(
        os.environ['ROADMAP_DATA'], "eia", "files", "ieo", "executive_summary.pdf")
    conf_path = os.path.join(
        os.environ['ROADMAP_DATA'], "eia", "files", "ieo", "executive_summary.conf")
    out_path = os.path.join(
        os.environ['ROADMAP_SCRAPER'], "ieo_test.json")
    # init converter
    c = EIAIEOConverter()
    c._parse_pdf_to_json(
        **{"filename": file_path, "config": conf_path, "outfile": out_path})



class EIAIEOConverter(RoadmapPDFConverter):
    dir_path = "../static/corpora/data/ieo"
    config_path = "../static/corpora/data/ieo/ieo_config.json"

    def __init__(self):
        with open(self.config_path, "r") as fp:
            config = json.load(fp)
        super().__init__(custom_filter=self._custom_filter, **config)

    def _custom_filter(self, line):
        # filter out IEO headers
        res = re.match(r"^(Table|Figure) [A-Z]?(\d+|\d+\-\d+)\.", line)
        if res:
            return ""
        else:
            return line


class EIAAEOConverter(RoadmapPDFConverter):
    dir_path = "../static/corpora/data/aeo"
    config_path = "../static/corpora/data/aeo/aeo_config.json"

    def __init__(self):
        with open(self.config_path, "r") as fp:
            config = json.load(fp)
        super().__init__(custom_filter=self._custom_filter, **config)

    def _custom_filter(self, line):
        # remove figure and table headers
        res = re.match(r"^(Table|Figure) ([A-Z]+)?([\d\-]+)\.", line)
        if res:
            return ""
        else:
            return line


def bulk_convert():
    eia_aeo = EIAAEOConverter()
    eia_ieo = EIAIEOConverter()

    processes = [
        Process(target=eia_aeo.mp_parse_multiple_to_json),
        Process(target=eia_ieo.mp_parse_multiple_to_json),
    ]
    for p in processes:
        p.start()
    for p in processes:
        p.join()


if __name__ == "__main__":
    bulk_convert()
