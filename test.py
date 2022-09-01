from asyncio import gather
from cgitb import text
# from utils.proceed import proceed
# from utils.information import *
import pandas as pd
import time
from bs4 import BeautifulSoup, SoupStrainer
import io
from tqdm import tqdm
import re
import pyarrow
tqdm.pandas()

OUTPUT_FILE='drug_concat_test.txt'
global_text = ""
otherTime = 0.0

def parseDefinitions(clean_text, source_name, concept_name, source_url, date_time_scraped, concept_type, CUI):
    # global global_text
    # global_text += ' \n SOURCEURLBEGIN ' + source_url + ' SOURCEURLEND\n DATETIMEBEGIN ' + date_time_scraped + ' DATETIMEEND\n SOURCENAMEBEGIN ' + source_name + ' SOURCENAMEEND\n CONCEPTTYPEBEGIN ' + concept_type + ' CONCEPTTYPEEND\n CUIBEGIN ' + str(CUI) + ' CUIEND\n DRUGBEGIN ' + concept_name + ' DRUGEND\n' + clean_text
    with io.open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
        f.write(' \n SOURCEURLBEGIN ' + source_url + ' SOURCEURLEND\n DATETIMEBEGIN ' + date_time_scraped + ' DATETIMEEND\n SOURCENAMEBEGIN ' + source_name + ' SOURCENAMEEND\n CONCEPTTYPEBEGIN ' + concept_type + ' CONCEPTTYPEEND\n CUIBEGIN ' + str(CUI) + ' CUIEND\n DRUGBEGIN ' + concept_name + ' DRUGEND\n' + clean_text)

print("> Gathering definitions...")
csv_path = "C:\\Users\\nickk\\Documents\\GitHub\\pilabsDefinitionExtraction\\clean_data.csv"

begin = time.time()
df = pd.read_parquet(csv_path, engine="pyarrow")
# df.to_parquet(csv_path, engine="pyarrow", compression="snappy")

end = time.time()
print('load time: ' + str(end - begin))

print(df.head(1))
beg_t = time.time()

print("> Extracting many definitions (this may take a while)...")
with io.open(OUTPUT_FILE,'w',encoding='utf-8') as f:
    f.write('')
df.progress_apply(lambda x : parseDefinitions(x["clean_text"], x["source_name"], x["name"], x["source_url"], x["date_time_scraped"], x["concept_type"], x["CUI"]), axis=1) 
        
# print("Other time: " + str(otherTime))

# parseDefinitions(x["raw_html"], x["source_name"], x["name"])
end_t = time.time()
print(f"> Total time: {end_t - beg_t}s")