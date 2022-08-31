from asyncio import gather
# from utils.proceed import proceed
# from utils.information import *
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
# from utils import info
from tqdm import tqdm
import time


filein = "C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_concatOutput.txt" 
fileout = "C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_concat_filtered_Output.csv"
newcsvout = "C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_maytreat_concat.csv"

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


# remove_fluff(filein,fileout)

def interpret_table():
    pd.set_option('display.max_rows', None)
    start = time.time()
    df = pd.read_csv(fileout, encoding='utf-8', sep='{S}', engine='python', nrows=None)
    end = time.time()
    print('Load time: '+str(end-start))

    start = time.time()
    df = df.fillna('')
    df = df.drop(['SOURCE_URL', 'DATE_TIME_SCRAPED', 'SOURCE_NAME'], axis=1)
    df = df.groupby('CUI')['MAY_TREAT'].agg(','.join).reset_index()
    print(df)
    end = time.time()
    print('Group by CUI time: '+str(end-start))

    df.to_csv(newcsvout, sep=str('|'))
    # print(df.drop_duplicates(subset=['CUI']).sort_values(by=['CUI']))
    # aggregate_func = {''}
    # df_merged = df.groupby(df['CUI']).aggregate()

    
interpret_table()