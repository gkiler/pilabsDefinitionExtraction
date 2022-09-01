from asyncio import gather
# from utils.proceed import proceed
# from utils.information import *
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
# from utils import info
from tqdm import tqdm
import time
import io
tqdm.pandas()

filein = "C:\\Users\\nickk\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_concat_testOutput.txt" 
fileout = "C:\\Users\\nickk\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_concat_filtered_Output.csv"
newcsvout = "C:\\Users\\nickk\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_maytreat_concat.csv"

def remove_fluff(filein,fileout):
    word_list= ["DRUGBEGIN", "DRUGEND", "SOURCEURLBEGIN", "SOURCEURLEND", "DATETIMEBEGIN", "DATETIMEEND", "CUIBEGIN", "CUIEND", "SOURCENAMEBEGIN", "SOURCENAMEEND", "CONCEPTTYPEBEGIN", "CONCEPTTYPEEND"]
    tempLine = ''
    # count = 0
    with open(filein, encoding='utf-8') as fin, open(fileout, "w+", encoding='utf-8') as fout:
        fout.write('SOURCE_URL{S}DATE_TIME_SCRAPED{S}SOURCE_NAME{S}CONCEPT_TYPE{S}CUI{S}DRUG_NAME{S}MAY_TREAT\n')
        for line in fin:
            if not any(word in line for word in word_list):
                line = line.replace("{S}", "")
                tempLine += line
                continue
            if "SOURCEURLBEGIN" in line:
                tempLine = tempLine.replace('\n', "") + "\n" 
                fout.write(tempLine)
                
                tempLine = ""
                for word in word_list:
                    line = line.replace(word, "")
                tempLine += line 
            else:
                for word in word_list:
                    line = line.replace(word, "")
                tempLine += line
            # ++count
        tempLine = tempLine.replace('\n', "") + "\n"
        fout.write(tempLine)


def drug_todo(drug_name, source_name):
    with io.open("TODO_drug.txt",'a',encoding='utf-8') as f:
        f.write(drug_name    + source_name + '\n')
def error_checking():
    pd.set_option('display.max_rows', None)
    df = pd.read_csv(fileout, encoding='utf-8', sep='{S}', engine='python', nrows=None)
    df = df.query('CONCEPT_TYPE == "  drug "')
    num_drugs = len(df)
    df = df.fillna('')
    df = df.loc[df['MAY_TREAT'] == ''].reset_index()
    df = df.drop(['index'],axis=1)
    with io.open("TODO_drug.txt",'w',encoding='utf-8') as f:
        f.write('')
    df.progress_apply(lambda x : drug_todo(x["DRUG_NAME"], x["SOURCE_NAME"]),axis=1)
    print(str(len(df))+" null values")
    print("may treat % found: " + str(1 - 1.0*len(df)/num_drugs))
def interpret_table():
    pd.set_option('display.max_rows', None)
    start = time.time()
    df = pd.read_csv(fileout, encoding='utf-8', sep='{S}', engine='python', nrows=None)
    end = time.time()
    print('Load time: '+str(end-start))

    start = time.time()
    df = df.fillna('')
    df = df.drop(['SOURCE_URL', 'DATE_TIME_SCRAPED', 'SOURCE_NAME'], axis=1)
    df = df.query('CONCEPT_TYPE == "  drug "')
    df = df.groupby(['CUI'])['MAY_TREAT'].agg('|'.join).reset_index()
    print(df.head(100))
    end = time.time()
    print('Group by CUI time: '+str(end-start))

    df.to_parquet(newcsvout, engine="pyarrow", compression="snappy")
    
def get_statistics():
    pd.set_option('display.max_rows', None)
    df = pd.read_parquet(newcsvout, engine="pyarrow")
    total_len = len(df)
    CUI_num = (df['MAY_TREAT'] =='').sum()
    print(df.head(100))
    print("total len: " + str(total_len))
    print("misses: " + str(CUI_num))
   
remove_fluff(filein,fileout) 
error_checking()
# interpret_table()
# get_statistics()
