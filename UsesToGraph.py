from asyncio import gather
# from utils.proceed import proceed
# from utils.information import *
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer

filein = "C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_concatOutput.txt" 
fileout = "C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_concatOutput.csv"
def remove_fluff(filein,fileout):
    word_list= ["DRUGBEGIN", "DRUGEND"]
    tempLine = ''
    # count = 0
    with open(filein, encoding='utf-8') as fin, open(fileout, "w+", encoding='utf-8') as fout:
        fout.write('DRUG_NAME{S}USES\n')
        for line in fin:
            line = line.replace("{S}", "")
            if "DRUGBEGIN" in line:
                # if count != 0:
                tempLine = tempLine.replace('\n', "") + "\n"
                
                fout.write(tempLine)
                # count = 0
                tempLine = ""
                for word in word_list:
                    line = line.replace(word, "")
                tempLine += line + "{S}"
            else:
                tempLine += line
            # ++count
        tempLine = tempLine.replace('\n', "") + "\n"
        fout.write(tempLine)

remove_fluff(filein,fileout)

def interpret_table():

    df = pd.read_csv("C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_concatOutput.csv", encoding='utf-8', sep='{S}', engine='python', nrows=None, index_col=[0])
    print(df.head(1000))

    
interpret_table()