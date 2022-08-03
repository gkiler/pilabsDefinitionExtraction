import os
from pickletools import read_uint1
import time
from multiprocessing import Pool
#requires Unitex installed

def openFile(fileLoc):
    openFileSNT = f"mkdir \"{fileLoc}_snt\""
    Normalize = f"UnitexToolLogger Normalize \"{fileLoc}.txt\" \"-rC:\\Users\\gwenk\\OneDrive\\Documents\\English\\Norm.txt\" \"--output_offsets={fileLoc}_snt\\normalize.out.offsets\" -qutf8-no-bom"
    Tokenize = f"UnitexToolLogger Tokenize \"{fileLoc}.snt\" \"-aC:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet.txt\" -qutf8-no-bom"
    Grf2Fst2 = f"UnitexToolLogger Grf2Fst2 \"C:\\Users\\gwenk\\OneDrive\\Documents\\English\\Graphs\\Preprocessing\\Sentence\\Sentence.grf\" -y \"--alphabet=C:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet.txt\" -qutf8-no-bom"
    Flatten = f"UnitexToolLogger Flatten \"C:\\Users\\gwenk\\OneDrive\\Documents\\English\\Graphs\\Preprocessing\\Sentence\\Sentence.fst2\" --rtn -d5 -qutf8-no-bom"
    Fst2Txt = f"UnitexToolLogger Fst2Txt \"-t{fileLoc}.snt\" \"C:\\Users\\gwenk\\OneDrive\\Documents\\English\\Graphs\\Preprocessing\\Sentence\\Sentence.fst2\" \"-aC:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet.txt\" -M \"--input_offsets={fileLoc}_snt\\normalize.out.offsets\" \"--output_offsets={fileLoc}_snt\\normalize.out.offsets\" -qutf8-no-bom"
    Grf2Fst2_2 = f"UnitexToolLogger Grf2Fst2 \"C:\\Users\\gwenk\\OneDrive\\Documents\\English\\Graphs\\Preprocessing\\Replace\\Replace.grf\" -y \"--alphabet=C:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet.txt\" -qutf8-no-bom"
    Fst2Txt_2 = f"UnitexToolLogger Fst2Txt \"-t{fileLoc}.snt\" \"C:\\Users\\gwenk\\OneDrive\\Documents\\English\\Graphs\\Preprocessing\\Replace\\Replace.fst2\" \"-aC:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet.txt\" -R \"--input_offsets={fileLoc}_snt\\normalize.out.offsets\" \"--output_offsets={fileLoc}_snt\\normalize.out.offsets\" -qutf8-no-bom"

    Dico = f"UnitexToolLogger Dico \"-t{fileLoc}.snt\" \"-aC:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet.txt\" \"C:\\Users\\gwenk\\AppData\\Local\\Unitex-GramLab\\English\\Dela\\dela-en-public.bin\" \"C:\\Users\\gwenk\\AppData\\Local\\Unitex-GramLab\\English\\Dela\\Dnum.fst2\" -qutf8-no-bom"
    SortTxt = f"UnitexToolLogger SortTxt \"{fileLoc}_snt\\dlf\" \"-l{fileLoc}_snt\\dlf.n\" \"-oC:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet_sort.txt\" -qutf8-no-bom"
    SortTxt2 = f"UnitexToolLogger SortTxt \"{fileLoc}_snt\\dlc\" \"-l{fileLoc}_snt\\dlc.n\" \"-oC:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet_sort.txt\" -qutf8-no-bom"
    SortTxt3 = f"UnitexToolLogger SortTxt \"{fileLoc}_snt\\err\" \"-l{fileLoc}_snt\\err.n\" \"-oC:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet_sort.txt\" -qutf8-no-bom"
    SortTxt4 = f"UnitexToolLogger SortTxt \"{fileLoc}_snt\\tags_err\" \"-l{fileLoc}_snt\\tags_err.n\" \"-oC:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet_sort.txt\" -qutf8-no-bom"

    os.system(openFileSNT)
    os.system(Normalize)
    os.system(Grf2Fst2)
    os.system(Flatten)
    os.system(Fst2Txt)
    os.system(Grf2Fst2_2)
    os.system(Fst2Txt_2)

    os.system(Tokenize)
    os.system(Dico)
    os.system(SortTxt)
    os.system(SortTxt2)
    os.system(SortTxt3)
    os.system(SortTxt4)

def runGraph(graphLoc, fileSNT, outputFile):
    runGraph = f"UnitexToolLogger Grf2Fst2 \"{graphLoc}.grf\" -y \"--alphabet=C:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet.txt\" -qutf8-no-bom"
    Locate = f"UnitexToolLogger Locate \"-t{fileSNT}.snt\" \"{graphLoc}.fst2\" \"-aC:\\Users\\gwenk\\OneDrive\\Documents\\English\\Alphabet.txt\" -L -I -n200 -b -Y --stack_max=1000 --max_matches_per_subgraph=200 --max_matches_at_token_pos=400 --max_errors=50 -qutf8-no-bom"
    Extract = f"UnitexToolLogger Extract --yes \"{fileSNT}.snt\" \"-i{fileSNT}_snt\\concord.ind\" \"-o{outputFile}\" -qutf8-no-bom"
    newDir = f"mkdir \"{fileSNT}_snt\" "

    os.system(runGraph)
    os.system(Locate)
    os.system(newDir)
    os.system(Extract)

def multip(graphLoc, name, outputFile):
    openFile(name)
    print('Running ' + name + '...')
    runGraph(graphLoc, name, outputFile)

if __name__ == '__main__':
    print("\nUnitex Python Script Wrapper\n")
    graphName = input("Input name of graph (no extension): ")
    # fileName = input("Input name of input file (no extension): ")

    graphLoc = "C:\\Users\\gwenk\\OneDrive\\Documents\\English\\Graphs\\" + graphName #defs  
    # fileLoc = "C:\\Users\\gwenk\\OneDrive\\Documents\\English\\Corpus\\" + fileName #concatPureText
    # outputFile = "C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\" #extractUses.txt

    drugPath = "C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\ReadFrom"
    out = "C:\\Users\\gwenk\\OneDrive\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\"
    beg_t = time.time()
    fileList = []
    for file in os.listdir(drugPath):
        if file.endswith(".txt"):
            filePath = drugPath + "\\" + file[:-4]
                # openFile(filePath)
                # runGraph(graphLoc, filePath, out+file)
            fileList.append(filePath)
    
    # for root, dirs, files in os.walk(drugPath):
    #     for file in files[:100]:
    #         if os.path.splitext(file)[1] == '.txt':
    #             filePath = drugPath + "\\" + file[:-4]
    #             # openFile(filePath)
    #             # runGraph(graphLoc, filePath, out+file)
    #             fileList.append(filePath)
    print(len(fileList))
    end_t = time.time()
    print(f"Total time: {end_t - beg_t}s") 


    p = Pool(processes=len(fileList))
    start = time.time()
    print("Begin work...")
    async_result = p.map_async(multip,graphLoc, fileList, out+file)
    p.close()
    p.join()
    end = time.time()
    print('Finished')
    print('Total time: '+str(end-start)+'s')
                   
    # openFile(fileLoc)
    # runGraph(graphLoc,fileLoc, outputFile)

