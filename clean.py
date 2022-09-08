from cgitb import text
from multiprocessing import Pool
import multiprocessing
import pandas as pd
import time
from bs4 import BeautifulSoup, SoupStrainer
import cchardet
from tqdm import tqdm
import re
import numpy as np
import os



tqdm.pandas()

CPU_COUNT = max(os.cpu_count() - 1, 1)
SOURCE_DIR = "C:\\Users\\\\Documents\\GitHub\\pilabsDefinitionExtraction\\"


def parse(raw_html):
    raw = BeautifulSoup(raw_html, "lxml").text
    raw_out = ""
    for line in raw.split('\n'):
        if line.strip() != '':
            raw_out += line + '\n'
    raw_out = re.sub(' +', ' ', raw_out)
    return raw_out

def parallelParse(df):
    # df.columns = []
    beg = time.time()
    num = str(multiprocessing.current_process()._identity)[1:-2]
    print("Begin processing df on "+num+"...")
    df["clean_text"] = df.apply(lambda x : parse(x["raw_html"]), axis=1)
    print("End processing df on "+num+"...")
    
    df = df.drop(["raw_html"], axis=1)
    
    print("Outputting to parquet file on "+num+"...")
    df.to_parquet(SOURCE_DIR+'\\Output\\Cleaning\\clean_data_part_'+num+'.parquet', engine='pyarrow',compression='snappy')
    
    print("Done on core "+num)
    end = time.time()
    print("Total time for core " + num + ": "+ str(end-beg) + "s")

if __name__ == '__main__':
    print("Opening file")
    # csv_path = "C:\\Users\\\\Documents\\GitHub\\pilabsDefinitionExtraction\\data.csv"
    # df = pd.read_csv(csv_path)
    #add other stuff
    # df.to_parquet('data.parquet',engine='pyarrow', compression='snappy')
    parquet_path = "C:\\Users\\\\Documents\\GitHub\\pilabsDefinitionExtraction\\data.parquet"
    df_start = pd.read_parquet(parquet_path, engine='pyarrow')
    
    
    beg = time.time()
    df_list = np.array_split(df_start, CPU_COUNT)
    print("Cleaning data on "+str(CPU_COUNT)+" cores")
    with Pool(CPU_COUNT) as pool:
        pool.map(parallelParse, df_list)
    end = time.time()
    print("Total time: " + str(end-beg) + "s")
    # df["clean_text"] = df.progress_apply(lambda x : parse(x["raw_html"]), axis=1)
    # df = df.drop(["raw_html"], axis=1)
    # df.to_parquet('clean_data.csv', engine='pyarrow')