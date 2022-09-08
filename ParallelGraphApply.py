import os
from pickletools import read_uint1
import time
import os
import pyarrow
import multiprocessing
from multiprocessing import Pool

CPU_COUNT = max(os.cpu_count() - 1, 1)
UNITEX_PATH="C:\\Users\\\\Documents\\Unitex-GramLab\\Unitex" #change to where "English" folder is installed
FILE_INPUT_DIR="C:\\Users\\\\Documents\\GitHub\\pilabsDefinitionExtraction\\" #change to file input directory
FILE_OUTPUT_DIR="C:\\Users\\\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\" #same as above for output
GRAMLAB_PATH="C:\\Users\\\\AppData\\Local" #change to source of UnitexGramlab folder 
BASE_INPUT="C:\\Users\\\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\Concat\\drug_concat_"

GRAPH_NAME='defs'

def openFile(fileLoc):
    openFileSNT = f"mkdir \"{fileLoc}_snt\""
    Normalize = f"UnitexToolLogger Normalize \"{fileLoc}.txt\" \"-r{UNITEX_PATH}\\English\\Norm.txt\" \"--output_offsets={fileLoc}_snt\\normalize.out.offsets\" -qutf8-no-bom"
    Tokenize = f"UnitexToolLogger Tokenize \"{fileLoc}.snt\" \"-a{UNITEX_PATH}\\English\\Alphabet.txt\" -qutf8-no-bom"
    Grf2Fst2 = f"UnitexToolLogger Grf2Fst2 \"{UNITEX_PATH}\\English\\Graphs\\Preprocessing\\Sentence\\Sentence.grf\" -y \"--alphabet={UNITEX_PATH}\\English\\Alphabet.txt\" -qutf8-no-bom"
    Flatten = f"UnitexToolLogger Flatten \"{UNITEX_PATH}\\English\\Graphs\\Preprocessing\\Sentence\\Sentence.fst2\" --rtn -d5 -qutf8-no-bom"
    Fst2Txt = f"UnitexToolLogger Fst2Txt \"-t{fileLoc}.snt\" \"{UNITEX_PATH}\\English\\Graphs\\Preprocessing\\Sentence\\Sentence.fst2\" \"-a{UNITEX_PATH}\\English\\Alphabet.txt\" -M \"--input_offsets={fileLoc}_snt\\normalize.out.offsets\" \"--output_offsets={fileLoc}_snt\\normalize.out.offsets\" -qutf8-no-bom"
    Grf2Fst2_2 = f"UnitexToolLogger Grf2Fst2 \"{UNITEX_PATH}\\English\\Graphs\\Preprocessing\\Replace\\Replace.grf\" -y \"--alphabet={UNITEX_PATH}\\English\\Alphabet.txt\" -qutf8-no-bom"
    Fst2Txt_2 = f"UnitexToolLogger Fst2Txt \"-t{fileLoc}.snt\" \"{UNITEX_PATH}\\English\\Graphs\\Preprocessing\\Replace\\Replace.fst2\" \"-a{UNITEX_PATH}\\English\\Alphabet.txt\" -R \"--input_offsets={fileLoc}_snt\\normalize.out.offsets\" \"--output_offsets={fileLoc}_snt\\normalize.out.offsets\" -qutf8-no-bom"
    
    os.system(openFileSNT)
    os.system(Normalize)
    os.system(Grf2Fst2)
    os.system(Flatten)
    os.system(Fst2Txt)
    os.system(Grf2Fst2_2)
    os.system(Fst2Txt_2)
    os.system(Tokenize)
        
    Dico = f"UnitexToolLogger Dico \"-t{fileLoc}.snt\" \"-a{UNITEX_PATH}\\English\\Alphabet.txt\" \"{GRAMLAB_PATH}\\Unitex-GramLab\\English\\Dela\\dela-en-public.bin\" \"{GRAMLAB_PATH}\\Unitex-GramLab\\English\\Dela\\Dnum.fst2\" -qutf8-no-bom"
    SortTxt = f"UnitexToolLogger SortTxt \"{fileLoc}_snt\\dlf\" \"-l{fileLoc}_snt\\dlf.n\" \"-o{UNITEX_PATH}\\English\\Alphabet_sort.txt\" -qutf8-no-bom"
    SortTxt2 = f"UnitexToolLogger SortTxt \"{fileLoc}_snt\\dlc\" \"-l{fileLoc}_snt\\dlc.n\" \"-o{UNITEX_PATH}\\English\\Alphabet_sort.txt\" -qutf8-no-bom"
    SortTxt3 = f"UnitexToolLogger SortTxt \"{fileLoc}_snt\\err\" \"-l{fileLoc}_snt\\err.n\" \"-o{UNITEX_PATH}\\English\\Alphabet_sort.txt\" -qutf8-no-bom"
    SortTxt4 = f"UnitexToolLogger SortTxt \"{fileLoc}_snt\\tags_err\" \"-l{fileLoc}_snt\\tags_err.n\" \"-o{UNITEX_PATH}\\English\\Alphabet_sort.txt\" -qutf8-no-bom"

    

    
    os.system(Dico)
    os.system(SortTxt)
    os.system(SortTxt2)
    os.system(SortTxt3)
    os.system(SortTxt4)

def runGraph(graphLoc, fileSNT, outputFile):
    runGraph = f"UnitexToolLogger Grf2Fst2 \"{graphLoc}.grf\" -y \"--alphabet={UNITEX_PATH}\\English\\Alphabet.txt\" -qutf8-no-bom"
    Locate = f"UnitexToolLogger Locate \"-t{fileSNT}.snt\" \"{graphLoc}.fst2\" \"-a{UNITEX_PATH}\\English\\Alphabet.txt\" -L -I --all -b -Y --stack_max=1000 --max_matches_per_subgraph=200 --max_matches_at_token_pos=400 --max_errors=50 -qutf8-no-bom"
    Extract = f"UnitexToolLogger Extract --yes \"{fileSNT}.snt\" \"-i{fileSNT}_snt\\concord.ind\" \"-o{outputFile}\" -qutf8-no-bom"
    newDir = f"mkdir \"{fileSNT}_snt\" "

    os.system(runGraph)
    os.system(Locate)
    os.system(newDir)
    os.system(Extract)

def parallelGraph(filename):
    beg = time.time()
    num = str(multiprocessing.current_process()._identity)[1:-2]
    
    graphLoc = f"{UNITEX_PATH}\\English\\Graphs\\" + GRAPH_NAME
    out = FILE_OUTPUT_DIR + "drug_extract_output_" + num + ".txt"
    
    openFile(filename)
    runGraph(graphLoc, filename, out)
    
    end = time.time()
    print("Total time for core " + num + ": "+ str(end-beg) + "s")

if __name__ == '__main__':
    # print("\nUnitex Python Script Wrapper\n")
    
    # fileName = input("Input name of input file (no extension): ")

    # graphLoc = f"{UNITEX_PATH}\\English\\Graphs\\" + graphName 
    # fileLoc = FILE_INPUT_DIR + fileName
    # outputFile = FILE_OUTPUT_DIR 

    # out = outputFile + fileName + "Output.txt"
    start = time.time()
    print("Begin work...")
    
    input_list = list()
    for i in range(1,CPU_COUNT+1):
        input = BASE_INPUT+str(i)
        input_list.append(input)
        
    # print()
    
    with Pool(CPU_COUNT) as pool:
        pool.map(parallelGraph, input_list)
    
    # if not os.path.exists(fileLoc+'_snt'):
    #     openFile(fileLoc)   
    # else:
    #     print(fileLoc+'_snt already exists')
        
    # runGraph(graphLoc, fileLoc, out)
    end = time.time()
    print('Finished')
    print('Total time: '+str(end-start)+'s')