from cgitb import text
import pandas as pd
import time
from bs4 import BeautifulSoup, SoupStrainer
import io
import cchardet
from tqdm import tqdm
import re
tqdm.pandas()




def parseDefinitions(raw_html):
    raw = BeautifulSoup(raw_html, "lxml").text
    raw_out = ""
    for line in raw.split('\n'):
        if line.strip() != '':
            raw_out += line + '\n'
    raw_out = re.sub(' +', ' ', raw_out)
    return raw_out

csv_path = ""
df_saved = pd.read_csv(csv_path, nrows=None)

df_saved["clean_text"] = df_saved.progress_apply(lambda x : parseDefinitions(x["raw_html"]), axis=1)
df_saved = df_saved.drop(["raw_html"], axis=1)
df_saved.to_parquet('clean_data.csv', engine='pyarrow')