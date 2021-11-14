# Principled Analysis of Energy Discourse across Domains with Thesaurus-based Automatic Topic Labeling
## Thomas Scelsi, Alfonso Martinez Arranz, Lea Frermann

This repository accompanies the above paper and provides:
1. Code to automatically create and enrich a dataset of roadmap documents from the Energy Information Administration (EIA) over the last 23 years
2. Analysis of DTM results on the two EIA corpora discussed in the results
3. Reference to the repository that contains the automatic labelling technique, split apart from this repo for easier maintaiability and generalisation.

## To create EIA dataset (~20min)

### 0. Clone repository
-----

```
user:~/$ git clone https://github.com/tscelsi/eia-diachronic-analysis.git
user:~/$ cd eia-diachronic-analysis
```

### 1. Create virtual environment and activate it (optional)
-----

```
user:~/eia-diachronic-analysis$ python -m venv <environ_name>

(linux)
user:~/eia-diachronic-analysis$ source <environ_name>/bin/activate

(windows)
user:~/eia-diachronic-analysis$ ./<environ_name>\Scripts\activate
```

### 2. Upgrade pip to latest version and install python package requirements
-----

```
user:~/eia-diachronic-analysis$ python -m pip install -U pip
user:~/eia-diachronic-analysis$ pip install -r requirements.txt
```

### 4. Install spacy "en_core_web_sm":
-----

```
user:~/eia-diachronic-analysis$ python -m spacy download en_core_web_sm
```

### 5. Create corpus
-----

```
user:~/eia-diachronic-analysis$ cd src
user:~/eia-diachronic-analysis/src$ python pipeline.py (~18min)
```

Once ```pipeline.py``` has finished running, the corpus can be accessed in the ```static/corpora/eia_corpora.pickle```. It can be loaded via python with the ```pandas.read_pickle()``` function.

## Automatic Labelling Techniques (Section 4.3 of paper)

The automatic labelling techniques are contained within the dtm-toolkit ```dtm_toolkit/auto_labelling.py```. These techniques can easily be generalised to other projects by cloning the dtm_toolkit and installing it as a package with ```pip install -e dtm_toolkit```, see dtm_toolkit README.md for more information.

## EuroVoc Label Filtering (Section 4.1 of paper)

All EuroVoc label filtering logic is contained in ```src/eurovoc_labelling/``` and ```static/eurovoc/eurovoc_final_labels.txt``` lists the final filtered labels both after qualitative and quantitative filtering as described in section 4.1 of the paper.

## Human Evaluation and DTM Results (Sections 6, 7 of paper)

Notebooks containing the majority of results for human evaluation of the automatic labelling techniques are contained in ```static/results/notebooks/```, further results for each model can be found in ```static/results/*_analysis*```.