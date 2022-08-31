import io
from tqdm import tqdm

INPUT_FILE="drug_concat_min.txt"

def RemoveDuplicateLines():
    lines_seen = set()
    with io.open('output' + '.txt', 'w', encoding='utf-8') as fout: 
        with io.open(INPUT_FILE, 'r', encoding='utf-8') as fin: 
            for line in tqdm(fin):
                h = hash(line)
                if h not in lines_seen:
                    fout.write(line)
                    lines_seen.add(h)

RemoveDuplicateLines()