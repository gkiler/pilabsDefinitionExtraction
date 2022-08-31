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

OUTPUT_FILE='drug_concat.txt'

lines_seen = set()
def parseDefinitions(raw_html, source_name, concept_name, source_url, date_time_scraped, concept_type, CUI):
    bs = BeautifulSoup(raw_html, "lxml")
    if len(bs.text) == 0:
        return None
    # raw = bs.text
    # raw_out = ""
    # for line in raw.split('\n'):
    #     h = hash(line)
    #     if h not in lines_seen:
    #         raw_out += line + '\n'
    #         lines_seen.add(h)
    string = ""
    string += ' \n SOURCEURLBEGIN ' + source_url + ' SOURCEURLEND\n'
    string += ' \n DATETIMEBEGIN ' + date_time_scraped + ' DATETIMEEND\n'
    string += ' \n SOURCENAMEBEGIN ' + source_name + ' SOURCENAMEEND\n'
    string += ' \n CONCEPTTYPEBEGIN ' + concept_type + ' CONCEPTTYPEEND\n' 
    string += ' \n CUIBEGIN ' + str(CUI) + ' CUIEND\n'
    string += ' \n DRUGBEGIN ' + concept_name + ' DRUGEND\n'
    string += bs.text
    with io.open(OUTPUT_FILE, 'a', encoding='utf-8') as f:  
        f.write(string) 

print("> Gathering definitions...")
csv_path = "C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\data.csv"
df_saved = pd.read_csv(csv_path, nrows=None)
print(df_saved.head(3))

beg_t = time.time()



print("> Extracting many definitions (this may take a while)...")
with io.open(OUTPUT_FILE,'w',encoding='utf-8') as f:
    f.write('')
df_saved["definition"] = df_saved.progress_apply(lambda x : parseDefinitions(x["raw_html"], x["source_name"], x["name"], x["source_url"], x["date_time_scraped"], x["concept_type"], x["CUI"]), axis=1)
# parseDefinitions(x["raw_html"], x["source_name"], x["name"])
end_t = time.time()
print(f"> Total time: {end_t - beg_t}s")


