from transformers import PegasusForConditionalGeneration, PegasusTokenizer
import time
import torch
import pandas as pd
import pyarrow
from tqdm import tqdm
tqdm.pandas()

#!pip install torch torchvision torchaudio --extra-index-url https://download.pytorch.org/whl/cu113
#!pip install transformers
#run in miniconda, use python not py or python3
# print(dir(torch.dml))
def Summarize(text):
    tokenizer = PegasusTokenizer.from_pretrained("google/pegasus-xsum")
    model = PegasusForConditionalGeneration.from_pretrained("google/pegasus-xsum").to("cpu")
    tokens = tokenizer(text, truncation=True, padding="longest", return_tensors="pt").to("cpu")
    summary = model.generate(**tokens)
    return tokenizer.decode(summary[0])

input_parquet = "C:\\Users\\\\Documents\\GitHub\\pilabsDefinitionExtraction\\Output\\drug_maytreat_concat.csv"
df = pd.read_parquet(input_parquet, engine='pyarrow')
df["sum"] = df.progress_apply(lambda x : Summarize(x["MAY_TREAT"]), axis=1)
exit()
#load tokenizer
print("Loading tokenizer...")
tokenizer = PegasusTokenizer.from_pretrained("google/pegasus-large")

beg = time.time()

print("Loading model...")
#load model
model = PegasusForConditionalGeneration.from_pretrained("google/pegasus-large").to("cuda")

text = """Dalfampridine is used to help improve walking in patients with multiple sclerosis.. Dalfampridine is used to help improve walking in patients with multiple sclerosis.. Dalfampridine is used to improve walking in patients with multiple sclerosis. Dalfampridine is used to improve walking in people who have multiple sclerosis (MS; a disease in which the nerves do not function properly and may cause weakness, numbness, loss of muscle coordination, and problems with vision, speech, and bladder control). Dalfampridine may be used alone or with other medications that control the symptoms of MS. Dalfampridine is in a class of medications called potassium channel blockers.. Dalfampridine is used to improve walking in people who have multiple sclerosis (MS; a disease in which the nerves do not function properly and may cause weakness, numbness, loss of muscle coordination, and problems with vision, speech, and bladder control). Dalfampridine may be used alone or with other medications that control the symptoms of MS. Dalfampridine is in a class of medications called potassium channel blockers.. Dalfampridine is used to improve walking in people who have multiple sclerosis (MS; a disease in which the nerves do not function properly and may cause weakness, numbness, loss of muscle coordination, and problems with vision, speech, and bladder control). Dalfampridine may be used alone or with other medications that control the symptoms of MS. Dalfampridine is in a class of medications called potassium channel blockers."""

print("Tokenizing...")
#tokenize text  
tokens = tokenizer(text, truncation=True, padding="longest", return_tensors="pt").to("cuda")

print("Summarizing...")
#run summary
summary = model.generate(**tokens)

print("Decoding...")
#output summary
print(tokenizer.decode(summary, skip_special_tokens=True))

end = time.time()

print("Total time: " + str(end - beg) + "s")