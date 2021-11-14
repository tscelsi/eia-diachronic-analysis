## Welcome to the scraping folder, this is where the collection of PDF Roadmap files occurs. In order to download the files, simply execute run.py

To change where pdf files are saved, one can assign save paths in the *config.json* file. Using one of these keys:
1. "eia_aeo_save_path": EIA Annual Energy Outlook save path
2. "eia_ieo_save_path": EIA International Energy Outlook save path
3. "eia_steo_save_path": EIA Short Term Energy Outlook save path
4. "irena_tech_brief_save_path": IRENA Technology Briefs save path

example *config.json*. 

    {
        "eia_steo_save_path": "C:\\Users\thomas\...\save_dir_name
    }

**to run full scrape:**

    python3 run.py

From the "scraping" folder.