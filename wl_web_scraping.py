import requests
from bs4 import BeautifulSoup
import pandas as pd
import regex as re
import time
from tqdm import tqdm  #licznik
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import gsheet_to_df

#%%

url = "https://redmine.wolnelektury.pl/projects/wl-publikacje/wiki/Rodziny_motyw%C3%B3w_i_grupy_tematyczne"
result = requests.get(url)
soup = BeautifulSoup(result.content, 'lxml')

test = soup.select('strong')
[e.text for e in test]

full_url = 'https://redmine.wolnelektury.pl/projects/wl-publikacje/wiki/Rodziny_motyw%C3%B3w_'

links = [[e.text, f"{full_url}{e.text.replace(' ', '_')}"] for e in test]

result_dict = {}
for label, link in tqdm(links):
    # label, link = links[0]
    r = requests.get(link)
    soup = BeautifulSoup(r.content, 'lxml')
    t = soup.select('.wiki-page .wiki-page')
    result_dict.update({label: [e.text for e in t]})


motifs_set = set([e for sub in [v for k,v in result_dict.items()] for e in sub])

final = {}
for e in motifs_set:
    # e = list(motifs_set)[0]
    final.update({e:{k for k,v in result_dict.items() if e in v}})

df = pd.DataFrame().from_dict(final, orient='index')

wl_df = gsheet_to_df('1t35DbYtJdEUrqhuvRJaOrVZ9HD1qqBfOjfV7R0bJRyE', 'lista WL')

df = df.reset_index(drop=False).rename(columns={'index': 'motif'})

df_final = pd.merge(wl_df, df, on='motif', how='outer')

len([e for e in wl_df['motif'].to_list() if e not in df['motif'].to_list()])

len([e for e in df['motif'].to_list() if e not in wl_df['motif'].to_list()])

df_final.to_excel('data/wl_motifs.xlsx', index=False)

df_over = pd.DataFrame().from_dict(result_dict, orient='index')
df_over.to_excel('data/wl_nadrzedne.xlsx')

















