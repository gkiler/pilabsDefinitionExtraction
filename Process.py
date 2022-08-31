from asyncio import gather
# from utils.proceed import proceed
# from utils.information import *
import pandas as pd
import time
from bs4 import BeautifulSoup, SoupStrainer
import io
from pathvalidate import sanitize_filename 
from tqdm import tqdm
tqdm.pandas()

CSV_SOURCE="C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\data.csv"

def gather_definitions():
    # do stuff to gather definitions
    print("> Gathering definitions...")

    df_saved = pd.read_csv(CSV_SOURCE, nrows=None)
    print(df_saved.head(3))
    
    beg_t = time.time()

    def parseDefinitions(raw_html, source_name, concept_name, source_url, date_time_scraped, concept_type, CUI):
        if "drugs.com" in source_name.lower(): # drugs.com is the source
            parse_only = SoupStrainer(attrs = {"class" : "contentBox"})
            bs = BeautifulSoup(raw_html, "lxml", parse_only=parse_only)
            mydivs = bs.find_all("div", attrs={"class": "contentBox"})
            string = ""
            if len(mydivs) == 0:
                return None
            interest_p = mydivs[0].find_all("p", attrs={"class" : None})[0:]
            string += ' \n SOURCEURLBEGIN ' + source_url + ' SOURCEURLEND\n'
            string += ' \n DATETIMEBEGIN ' + date_time_scraped + ' DATETIMEEND\n'
            string += ' \n SOURCENAMEBEGIN ' + source_name + ' SOURCENAMEEND\n'
            string += ' \n CONCEPTTYPEBEGIN ' + concept_type + ' CONCEPTTYPEEND\n' 
            string += ' \n CUIBEGIN ' + str(CUI) + ' CUIEND\n'
            string += ' \n DRUGBEGIN ' + concept_name + ' DRUGEND\n'
            for p in interest_p:
                string += p.text
            with io.open('drug_concat' + '.txt', 'a', encoding='utf-8') as f:  
                f.write(string) 
        elif "mayoclinic" in source_name.lower(): # Mayoclinic is the source
            parse_only = SoupStrainer(attrs = {"id" : "main-content"})
            bs = BeautifulSoup(raw_html, "lxml", parse_only=parse_only)
            mydivs = bs.find_all("div", attrs={"id" : "main-content"})
            string = ""
            if len(mydivs) == 0:
                return None
            interest_p = mydivs[0].find_all("p", attrs={"class" : None})[0:]
            string += ' \n SOURCEURLBEGIN ' + source_url + ' SOURCEURLEND\n'
            string += ' \n DATETIMEBEGIN ' + date_time_scraped + ' DATETIMEEND\n'
            string += ' \n SOURCENAMEBEGIN ' + source_name + ' SOURCENAMEEND\n'
            string += ' \n CONCEPTTYPEBEGIN ' + concept_type + ' CONCEPTTYPEEND\n' 
            string += ' \n CUIBEGIN ' + str(CUI) + ' CUIEND\n'
            string += ' \n DRUGBEGIN ' + concept_name + ' DRUGEND\n'
            for p in interest_p:
                string += p.text
            with io.open('drug_concat' + '.txt', 'a', encoding='utf-8') as f:  
                f.write(string) 
        elif "webmd" in source_name.lower(): # WebMD is the source
            parse_only = SoupStrainer(attrs = {"class" : "monograph-content monograph-content-holder"})
            bs = BeautifulSoup(raw_html, "lxml", parse_only=parse_only)
            mydivs = bs.find_all("div", attrs={"class" : "monograph-content monograph-content-holder"})
            string = ""
            if len(mydivs) == 0:
                return None
            interest_p = mydivs[0].find_all("p", attrs={"class" : None})[0:]
            string += ' \n SOURCEURLBEGIN ' + source_url + ' SOURCEURLEND\n'
            string += ' \n DATETIMEBEGIN ' + date_time_scraped + ' DATETIMEEND\n'
            string += ' \n SOURCENAMEBEGIN ' + source_name + ' SOURCENAMEEND\n'
            string += ' \n CONCEPTTYPEBEGIN ' + concept_type + ' CONCEPTTYPEEND\n' 
            string += ' \n CUIBEGIN ' + str(CUI) + ' CUIEND\n'
            string += ' \n DRUGBEGIN ' + concept_name + ' DRUGEND\n'
            for p in interest_p:
                string += p.text
            with io.open('drug_concat' + '.txt', 'a', encoding='utf-8') as f:  
                f.write(string) 
        elif "medline" in source_name.lower(): # Medline is the source
            # section-body
            parse_only = SoupStrainer(attrs = {"class" : "section-body"})
            bs = BeautifulSoup(raw_html, "lxml", parse_only=parse_only)
            mydivs = bs.find_all("div", attrs={"class" : "section-body"})
            string = ""
            if len(mydivs) == 0:
                return None
            interest_p = mydivs[0].find_all("p", attrs={"class" : None})[0:]
            string += ' \n SOURCEURLBEGIN ' + source_url + ' SOURCEURLEND\n'
            string += ' \n DATETIMEBEGIN ' + date_time_scraped + ' DATETIMEEND\n'
            string += ' \n SOURCENAMEBEGIN ' + source_name + ' SOURCENAMEEND\n'
            string += ' \n CONCEPTTYPEBEGIN ' + concept_type + ' CONCEPTTYPEEND\n' 
            string += ' \n CUIBEGIN ' + str(CUI) + ' CUIEND\n'
            string += ' \n DRUGBEGIN ' + concept_name + ' DRUGEND\n'
            for p in interest_p:
                string += p.text
            with io.open('drug_concat' + '.txt', 'a', encoding='utf-8') as f:  
                f.write(string) 
        elif "cdc" in source_name.lower():
            # section-body
            parse_only = SoupStrainer(attrs = {"class" : "col-md-12 splash-col"})
            bs = BeautifulSoup(raw_html, "lxml", parse_only=parse_only)
            mydivs = bs.find_all("div", attrs={"class" : "col-md-12 splash-col"})
            string = ""
            if len(mydivs) == 0:
                return None
            interest_p = mydivs[0].find_all("p", attrs={"class" : None})[0:]
            string += ' \n SOURCEURLBEGIN ' + source_url + ' SOURCEURLEND\n'
            string += ' \n DATETIMEBEGIN ' + date_time_scraped + ' DATETIMEEND\n'
            string += ' \n SOURCENAMEBEGIN ' + source_name + ' SOURCENAMEEND\n'
            string += ' \n CONCEPTTYPEBEGIN ' + concept_type + ' CONCEPTTYPEEND\n' 
            string += ' \n CUIBEGIN ' + str(CUI) + ' CUIEND\n'
            string += ' \n DRUGBEGIN ' + concept_name + ' DRUGEND\n'
            for p in interest_p:
                string += p.text
            with io.open('drug_concat' + '.txt', 'a', encoding='utf-8') as f:  
                f.write(string) 
        elif "nhs" in source_name.lower():
            # section-body
            parse_only = SoupStrainer(attrs = {"class" : "js-guide cf guide"})
            bs = BeautifulSoup(raw_html, "lxml", parse_only=parse_only)
            mydivs = bs.find_all("div", attrs={"class" : "tab js-guide__section guide__section active"})
            string = ""
            if len(mydivs) == 0:
                return None
            interest_p = mydivs[0].find_all("p", attrs={"class" : None})[0:]
            string += ' \n SOURCEURLBEGIN ' + source_url + ' SOURCEURLEND\n'
            string += ' \n DATETIMEBEGIN ' + date_time_scraped + ' DATETIMEEND\n'
            string += ' \n SOURCENAMEBEGIN ' + source_name + ' SOURCENAMEEND\n'
            string += ' \n CONCEPTTYPEBEGIN ' + concept_type + ' CONCEPTTYPEEND\n' 
            string += ' \n CUIBEGIN ' + str(CUI) + ' CUIEND\n'
            string += ' \n DRUGBEGIN ' + concept_name + ' DRUGEND\n'
            for p in interest_p:
                string += p.text
            with io.open('drug_concat' + '.txt', 'a', encoding='utf-8') as f:  
                f.write(string) 
        else:
            return None
        # else:
        #     return None

    print("> Extracting many definitions (this may take a while)...")
    with io.open('drug_concat.txt','w',encoding='utf-8') as f:
        f.write('')
    df_saved["definition"] = df_saved.progress_apply(lambda x : parseDefinitions(x["raw_html"], x["source_name"], x["name"], x["source_url"], x["date_time_scraped"], x["concept_type"], x["CUI"]), axis=1)
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
