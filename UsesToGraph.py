from asyncio import gather
# from utils.proceed import proceed
# from utils.information import *
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer

filein = "C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_concatOutput.txt" 
fileout = "C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_concatOutput.csv"
def remove_fluff(filein,fileout):
    word_list= ["DRUGBEGIN", "DRUGEND", "SOURCEURLBEGIN", "SOURCEURLEND", "DATETIMEBEGIN", "DATETIMEEND", "CUIBEGIN", "CUIEND", "SOURCENAMEBEGIN", "SOURCENAMEEND", "CONCEPTTYPEBEGIN", "CONCEPTTYPEEND"]
    tempLine = ''
    # count = 0
    with open(filein, encoding='utf-8') as fin, open(fileout, "w+", encoding='utf-8') as fout:
        fout.write('SOURCE_URL{S}DATE_TIME_SCRAPED{S}SOURCE_NAME{S}CONCEPT_TYPE{S}CUI{S}DRUG_NAME{S}USES\n')
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

remove_fluff(filein,fileout)

def interpret_table():
    pd.set_option('display.max_rows', None)
    df = pd.read_csv("C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_concatOutput.csv", encoding='utf-8', sep='{S}', engine='python', nrows=None, index_col=[0])
    print(df)

    
interpret_table()