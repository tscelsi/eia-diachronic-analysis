"""
This file contains the DataFrameCreator class. This class converts structured json files
from directories it is pointed to (dirs arg in __init__ function) into one large dataframe. 
This dataframe is saved in save_path.

To run: python3 dataframe.py
"""

from collections import Counter
import pandas as pd
import json
import os
from multiprocessing import Pool
import re
from time import time
import spacy
from spacy.tokens import DocBin
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from dtm_toolkit.preprocessing import Preprocessing

DOC_YEAR_MAP_PATH = "../static/corpora/doc_year_map.json"
NUM_CPU = os.cpu_count() - 1 if os.cpu_count() > 1 else 1


def remove_unserializable_results(doc):
        doc.user_data = {}
        for x in dir(doc._):
            if x in ['get', 'set', 'has']: continue
            setattr(doc._, x, None)
        for token in doc:
            for x in dir(token._):
                if x in ['get', 'set', 'has']: continue
                setattr(token._, x, None)
        return doc

class DataFrameCreator:
    def __init__(self, dirs, save_path="corpus_df.csv"):
        if isinstance(dirs, list):
            self.dirs = dirs
        elif isinstance(dirs, str):
            self.dirs = [dirs]
        print(f"Fetching _structured.json files from the following directories:")
        print("=============")
        for x in self.dirs:
            print(f"-- {x}")
        self.files = self._get_files()
        print("=============")
        print(f"Found {len(self.files)} files.")
        self.nlp = spacy.load("en_core_web_sm")
        self.nlp.add_pipe('sentencizer')
        self.columns = ["organisation", "doc_category", "filename",
                        "header_text", "para_text", "header_size", "para_size", "start_page"]
        self.save_path = save_path

    def run(self, type_="csv", tfidf=True, lemm_pos=True, save=True):
        """This function runs the dataframe creator. It combines the content of the structured json files into header-paragraph pairs,
        and places them into a dataframe. There are also optional enrichment steps that are executed on the raw data
        in order to create more useful columns for analysis further down the pipeline.

        Args:
            type_ (str, optional): This is the save type of the dataframe. Defaults to "csv".
            tfidf (bool, optional): Whether or not to enrich the dataframe with a tfidf representation of each paragraph. Defaults to True.
            lemm_pos (bool, optional): Whether or not to enrich the dataframe with a lemmatised and pos tagged representation of each paragraph. 
                NOTE takes a significant amount of time to run. Defaults to True.
        """
        # combine text into header-paragraph pairs
        print("combining...")
        data = self.combine()
        # create initial data frame with the combined information
        print("creating dataframe...")
        df = pd.DataFrame(data=data, columns=self.columns)
        df = df.dropna(subset=['para_text'])
        print("enriching...")
        df = self._enrich(df, tfidf=tfidf, lemm_pos=lemm_pos)
        df = self.annotate_year(DOC_YEAR_MAP_PATH, df) 
        if save:
            if type_ == "csv":
                df.to_csv(self.save_path, index=False)
            else:
                df.to_pickle(self.save_path)
        return df

    def combine(self):
        """Merges all the documents into header paragraph pairs and creates a dataframe
        """
        combined = []
        pool = Pool(NUM_CPU)
        a = [pool.apply_async(self.combine_doc, args=(path,))
             for path in self.files]
        for fut in a:
            res = fut.get()
            if res:
                combined.extend(res)
        print(f"Found {len(combined)} header-paragraph pairings in the corpus.")
        return combined

    @classmethod
    def annotate_year(self, mapping_path, df):
        with open(mapping_path, "r") as fp:
            mapping = json.load(fp)
        years = []
        null_counter = 0
        for file_ in df['filename']:
            if file_ not in mapping:
                null_counter += 1
                years.append(None)
            else:
                years.append(int(mapping[file_]))
        df['year'] = years
        return df


    def _enrich(self, df, tfidf=True, lemm_pos=True):
        """This function enriches the dataframes representation of each paragraph by adding columns such as: 
        1. The highest ranking tfidf words from each paragraph based on the whole dataframe corpus. 
        2. Spacy document object that can be used for pos tags, dependency parsing etc.
        3. if needed, a list representation of each word in the paragraph with it's respective pos tag and lemmatised form.

        In all cases, this function provides basic enrichment by creating columns with filtered headers and filtered paragraphs that
        remove most puncutation, stopwords etc. (see corpus.py). As well as this, it also provides a column that contains a spacy representation of each 
        paragraph.

        NOTE the spacy object for each paragraph will only persist if used with type_ = "pickle"

        Args:
            df (pd.DataFrame): the dataframe that holds the raw paragraph data.

        Returns:
            df (pd.DataFrame): the dataframe that now holds the raw and enriched paragraph data.
        """
        df = df.dropna(subset=['para_text'])
        header_preprocessor = Preprocessing(self.nlp.pipe(df['header_text'], n_process=NUM_CPU, batch_size=256))
        para_preprocessor = Preprocessing(self.nlp.pipe(df['para_text'], n_process=NUM_CPU, batch_size=256))
        # simple tokenisation, no n-grams
        header_preprocessor.preprocess(ngrams=False)
        para_preprocessor.preprocess(ngrams=False)
        filtered_headers = pd.Series(header_preprocessor.get_merged_docs(keep_empty=True), dtype="string")
        filtered_paras = pd.Series(para_preprocessor.get_merged_docs(keep_empty=True), dtype="string")
        df['filt_header_text'] = filtered_headers.fillna("").to_list()
        df['filt_para_text'] = filtered_paras.fillna("").to_list()
        if tfidf or lemm_pos:
            self.enricher = Enricher()
            if lemm_pos:
                df = self.enricher.get_lemm_pos_para_text(df)
            if tfidf:
                df = self.enricher.get_tfidf(df)
        return df

    def _get_files(self):
        files = []
        for path in self.dirs:
            dir_files = [os.path.join(path, f) for f in os.listdir(
                path) if re.match(r".*_structured.json", f)]
            print(f"Found {len(dir_files)} in {path}")
            files.extend(dir_files)
        return files

    def combine_doc(self, file_path):
        """Merges a single document into header-paragraph pairs by:
            1. Checking to see if the current chunk is a heading
            2. If it is, then it gets a new entry in the accumulator list
            3. if it is not, then checks to see if previous element was heading, 
                if yes, then we add them as a pair to list, if not then we add just the header with no paragraph text.
            Return list of lists with form: [organisation, document_category, filename, header_text, para_text, header_size, para_size, start_page]
        """
        paired_data = []
        try:
            with open(file_path, "r") as rfp:
                data = json.load(rfp)['doc']
        except Exception as e:
            print("error opening structured json file.")
            print(e)
            return None
        org, category, fn = DataFrameCreator.get_path_metadata(file_path)
        if len(data) == 1 and data[0] == {}:
            return None
        else:
            std_font_size = self._get_para_font_size(data)
        for i in range(len(data)):
            el = data[i]
            el_is_heading = self._is_heading(el['size'], std_font_size)
            # edge case at end of data array
            if i == len(data) - 1:
                if el_is_heading:
                    paired_data.append(
                        [org, category, fn, el['text'], None, el['size'], None, el['page']])
                # else do nothing; the final paragraph element should have been picked up by the previous heading
            else:
                next_el = data[i+1]
                next_el_is_heading = self._is_heading(
                    next_el['size'], std_font_size)
                if el_is_heading and next_el_is_heading:
                    # then there is no paragraph text for the current element
                    paired_data.append(
                        [org, category, fn, el['text'], None, el['size'], None, el['page']])
                elif el_is_heading and not next_el_is_heading:
                    # this signifies that the next element is a paragraph of text to be matched with the current heading.
                    paired_data.append(
                        [org, category, fn, el['text'], next_el['text'], el['size'], next_el['size'], el['page']])
                elif not el_is_heading and not next_el_is_heading:
                    paired_data.append(
                        [org, category, fn, None, next_el['text'], None, next_el['size'], next_el['page']])
        return paired_data

    def _get_para_font_size(self, doc):
        acc = Counter()
        for el in doc:
            acc[el['size']] += len(el['text'])
        return acc.most_common(1)[0][0]

    def _is_heading(self, size, std_font_size):
        if size > std_font_size:
            return True
        else:
            return False

    @staticmethod
    def get_path_metadata(path, is_dir=False):
        """Extracts the organisation from a path variable of the form:
        C:/Users/thomas/Documents/Thesis/coding/eia/files/ieo/0484(2016)_structured.json

        Args:
            path (str): as above.
        """
        relpath = os.path.relpath(path, '../static/corpora/data')
        toks = re.split(r"[\\/]", relpath)
        org = "EIA"
        doc_category = toks[0]
        file_name = toks[1]
        return (org, doc_category, file_name)


class Enricher:
    def __init__(self, tfidf_max_lim=100):
        self.nlp = spacy.load("en_core_web_sm")
        self.tfidf_max_lim = tfidf_max_lim

    def get_tfidf(self, df):
        """ Gets a list of the highest value tfidfs from the corpus for each row and returns these values with their pos tags.

        Args:
            df ([type]): [description]
        """
        v = TfidfVectorizer()
        tdm = v.fit_transform(df['filt_para_text'].to_list())
        doc_tfidfs = []
        for i, doc in enumerate(tdm):
            sorted_terms = [g for g, v in sorted(zip(list(v.get_feature_names(
            )), doc.todense().tolist()[0]), key=lambda x: x[1], reverse=True) if v != 0]
            terms = sorted_terms[:self.tfidf_max_lim]
            doc_tfidfs.append(terms)
        df['para_tfidf'] = doc_tfidfs
        return df

    def get_lemm_pos_para_text(self, df):
        rows = []
        docs = self.nlp.pipe(df.filt_para_text, n_process=NUM_CPU, batch_size=256)
        for doc in docs:
            lem_pos_row = []
            for tok in doc:
                lem_pos_row.append((tok.lemma_, tok.pos_))
            rows.append(lem_pos_row)
        df['lemm_pos_filt_para_text'] = pd.Series(rows)
        return df


def test_enricher():
    df = pd.read_csv("header_para_df.csv").head(30)
    del df['para_tfidf']
    e = Enricher()
    df = e.get_tfidf(df.fillna(""))


def test_combine():
    file_path = os.path.join(os.environ['ROADMAP_DATA'], "eia", "files", "steo", "apr14_structured.json")
    dfc = DataFrameCreator([], save_path=f"greyroads_df.csv")
    result = dfc.combine_doc(file_path)
    breakpoint()

def main():
    start = time()
    dirs = [
        os.path.join(os.environ['ROADMAP_DATA'], "eia", "files", "ieo"),
        os.path.join(os.environ['ROADMAP_DATA'], "eia", "files", "aeo"),
    ]
    dfc = DataFrameCreator(dirs, save_path=f"../static/corpora/data/converted_df.csv")
    dfc.run(lemm_pos=False, tfidf=False)
    end = time()
    print(f"{end - start}")


if __name__ == "__main__":
    main()
