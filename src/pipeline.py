from dataframe import DataFrameCreator
from converter import bulk_convert
from dtm_toolkit.lucy import run_measuring_space
from data_collection.run import run
import json



def create_dataframe_from_structured(enrich=True, save=False):
    dirs = [
        "../static/corpora/data/ieo",
        "../static/corpora/data/aeo"
    ]
    dfc = DataFrameCreator(dirs, save_path=f"eia_df.csv")
    if enrich:
        df = dfc.run(lemm_pos=True, tfidf=True, save=save)
    else:
        df = dfc.run(lemm_pos=False, tfidf=False, save=save)
    return df


def pipeline(run_scrape=True, run_conversion=True, run_dataframe_creation=True, enrich=True, run_lucy=True):
    # run scrape
    if run_scrape:
        run()
    if run_conversion:
        bulk_convert()
    if run_dataframe_creation:
        df = create_dataframe_from_structured(enrich, save=False)
        if run_lucy:
            with open("../static/corpora/energy_technology.json") as fp:
                matcher_keywords = json.load(fp)
            breakpoint()
            enriched_df = run_measuring_space(df, "para_text", matcher_keywords, save=False)
        enriched_df = enriched_df.dropna(subset=["year"])
        enriched_df.to_pickle("../static/corpora/eia_dataset.pickle", protocol=4)

if __name__ == "__main__":
    pipeline()

