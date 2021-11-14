from dataframe import DataFrameCreator
from dtm_toolkit.lucy import Lucy
from helper.helper import timer
from scraping.run import run
import os


def create_dataframe_from_structured(enrich=True):
    dirs = [
        "../static/corpora/data/ieo/",
        "../static/corpora/data/aeo/"
    ]
    dfc = DataFrameCreator(dirs, save_path=f"eia_df.csv")
    if enrich:
        df = dfc.run(lemm_pos=True, tfidf=True)
    else:
        df = dfc.run(lemm_pos=False, tfidf=False)
    return df


def pipeline(run_scrape=True, run_dataframe_creation=True, enrich=True, run_lucy=True):
    # run scrape
    if run_scrape:
        run()
    if run_dataframe_creation:
        df = create_dataframe_from_structured(enrich)
        if run_lucy:
            lucy = Lucy()
            df = lucy.run_measuring_space(df, "para_text")
        df = df.dropna(subset=["year"])
        df.to_pickle("eia_annotated.pickle", protocol=4)

if __name__ == "__main__":
    pipeline()

