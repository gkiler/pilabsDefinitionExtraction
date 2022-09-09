from asyncio import gather
from cgitb import text
# from utils.proceed import proceed
# from utils.information import *
import pandas as pd
import time
from bs4 import BeautifulSoup, SoupStrainer
import io
from tqdm import tqdm
import os
import pyarrow
import multiprocessing
from multiprocessing import Pool

tqdm.pandas()
CPU_COUNT = max(os.cpu_count() - 1, 1)

SOURCE_DIR = "C:\\Users\\\\Documents\\GitHub\\pilabsDefinitionExtraction\\"
OUTPUT_FILE='drug_concat_test.txt'
OUTPUT_BASE=SOURCE_DIR+'\\Output\\Concat\\drug_concat_'
INPUT_BASE=SOURCE_DIR+'\\Output\\Cleaning\\clean_data_part_'
otherTime = 0.0

def parseDefinitions(fout, clean_text, source_name, concept_name, source_url, date_time_scraped, concept_type, CUI):
    # global global_text
    # global_text += ' \n SOURCEURLBEGIN ' + source_url + ' SOURCEURLEND\n DATETIMEBEGIN ' + date_time_scraped + ' DATETIMEEND\n SOURCENAMEBEGIN ' + source_name + ' SOURCENAMEEND\n CONCEPTTYPEBEGIN ' + concept_type + ' CONCEPTTYPEEND\n CUIBEGIN ' + str(CUI) + ' CUIEND\n DRUGBEGIN ' + concept_name + ' DRUGEND\n' + clean_text
    with io.open(fout, 'a', encoding='utf-8') as f:
        f.write(' \n SOURCEURLBEGIN ' + source_url + ' SOURCEURLEND\n DATETIMEBEGIN ' + date_time_scraped + ' DATETIMEEND\n SOURCENAMEBEGIN ' + source_name + ' SOURCENAMEEND\n CONCEPTTYPEBEGIN ' + concept_type + ' CONCEPTTYPEEND\n CUIBEGIN ' + str(CUI) + ' CUIEND\n DRUGBEGIN ' + concept_name + ' DRUGEND\n' + clean_text)

def parallelParse(df):
    beg = time.time()
    
    num = str(multiprocessing.current_process()._identity)[1:-2]
    fout = OUTPUT_BASE+num+'.txt'
    with io.open(fout, 'w',encoding='utf-8') as f:
        f.write("")
    df.progress_apply(lambda x : parseDefinitions(fout, x["clean_text"], x["source_name"], x["name"], x["source_url"], x["date_time_scraped"], x["concept_type"], x["CUI"]), axis=1) 
    
    end = time.time()
    print("Total time for core " + num + ": "+ str(end-beg) + "s")

# csv_path = "C:\\Users\\\\Documents\\GitHub\\pilabsDefinitionExtraction\\clean_data.csv"

# begin = time.time()
# df = pd.read_parquet(csv_path, engine="pyarrow")

# df.to_parquet(csv_path, engine="pyarrow", compression="snappy")

# end = time.time()
# print('load time: ' + str(end - begin))

# print(df.head(1))
# beg_t = time.time()
if __name__ == '__main__':
    begin = time.time()

    df_list = list()
    for i in range(1,CPU_COUNT+1):
        df = pd.read_parquet(INPUT_BASE+str(i)+".parquet",engine='pyarrow')
        df_list.append(df)

    with Pool(CPU_COUNT) as pool:
        pool.map(parallelParse, df_list)
            
    # with io.open(OUTPUT_FILE,'w',encoding='utf-8') as f:
    #     f.write('')
    # df.progress_apply(lambda x : parseDefinitions(x["clean_text"], x["source_name"], x["name"], x["source_url"], x["date_time_scraped"], x["concept_type"], x["CUI"]), axis=1) 
            
    # print("Other time: " + str(otherTime))

    # parseDefinitions(x["raw_html"], x["source_name"], x["name"])
    end = time.time()
    print("Total time: " + str(end-begin) + "s")