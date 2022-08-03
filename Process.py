from asyncio import gather
# from utils.proceed import proceed
# from utils.information import *
import pandas as pd
import time
from bs4 import BeautifulSoup, SoupStrainer
import io
from pathvalidate import sanitize_filename 

def gather_definitions():
    # do stuff to gather definitions
    print("> Gathering definitions...")

    df_saved = pd.read_csv("C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\data.csv", nrows=None)
    df_saved.head(3)

    beg_t = time.time()

    def parseDefinitions(raw_html, source_name, concept_name):
        if source_name == "drugs.com": # drugs.com is the source
            parse_only = SoupStrainer(attrs = {"class" : "contentBox"})
            bs = BeautifulSoup(raw_html, "lxml", parse_only=parse_only)
            mydivs = bs.find_all("div", attrs={"class": "contentBox"})
            string = ""
            if len(mydivs) == 0:
                return None
            interest_p = mydivs[0].find_all("p", attrs={"class" : None})[0:]
            for p in interest_p:
                string += p.text
            with io.open("ReadFrom\\"+sanitize_filename(concept_name) + '.txt', 'w', encoding='utf-8') as f:
                f.write(string) 
        elif source_name == "Mayoclinic": # Mayoclinic is the source
            parse_only = SoupStrainer(attrs = {"id" : "main-content"})
            bs = BeautifulSoup(raw_html, "lxml", parse_only=parse_only)
            mydivs = bs.find_all("div", attrs={"id" : "main-content"})
            string = ""
            if len(mydivs) == 0:
                return None
            interest_p = mydivs[0].find_all("p", attrs={"class" : None})[0:]
            for p in interest_p:
                string += p.text
            with io.open("ReadFrom\\"+sanitize_filename(concept_name) + '.txt', 'w', encoding='utf-8') as f:  
                f.write(string) 
        elif source_name == "WebMD": # WebMD is the source
            parse_only = SoupStrainer(attrs = {"class" : "monograph-content monograph-content-holder"})
            bs = BeautifulSoup(raw_html, "lxml", parse_only=parse_only)
            mydivs = bs.find_all("div", attrs={"class" : "monograph-content monograph-content-holder"})
            string = ""
            if len(mydivs) == 0:
                return None
            interest_p = mydivs[0].find_all("p", attrs={"class" : None})[0:]
            for p in interest_p:
                string += p.text
            with io.open("ReadFrom\\"+sanitize_filename(concept_name) + '.txt', 'w', encoding='utf-8') as f:  
                f.write(string) 
        elif source_name == "Medline": # Medline is the source
            # section-body
            parse_only = SoupStrainer(attrs = {"class" : "section-body"})
            bs = BeautifulSoup(raw_html, "lxml", parse_only=parse_only)
            mydivs = bs.find_all("div", attrs={"class" : "section-body"})
            string = ""
            if len(mydivs) == 0:
                return None
            interest_p = mydivs[0].find_all("p", attrs={"class" : None})[0:]
            for p in interest_p:
                string += p.text
            with io.open("ReadFrom\\"+sanitize_filename(concept_name) + '.txt', 'w', encoding='utf-8') as f:  
                f.write(string) 
        return ""
        # else:
        #     return None

    print("> Extracting many definitions (this may take a while)...")
    df_saved["definition"] = df_saved.apply(lambda x : parseDefinitions(x["raw_html"], x["source_name"], x["name"]), axis=1)
    # parseDefinitions(x["raw_html"], x["source_name"], x["name"])
    end_t = time.time()
    print(f"> Total time: {end_t - beg_t}s")

    # beginning = time.time()
    # print(f"> Saving...")
    # df_saved.reset_index()
    # # df_saved.to_csv(DATA_PATH + "/" + OUT_FILE, index=False)
    # final = time.time()
    # print(f"> Done saving.")
    # print(f"> Time taken: {final - beginning}s")

gather_definitions()
