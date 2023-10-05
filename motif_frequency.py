import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
import requests
from tqdm import tqdm
import numpy as np
from my_functions import gsheet_to_df, simplify_string, cluster_strings, marc_parser_to_dict
from concurrent.futures import ThreadPoolExecutor
import json
import glob
from geonames_accounts import geonames_users
import random
import time
from datetime import datetime
import Levenshtein as lev
import io

#%%

path = 'D:/IBL/BN/bn_all/2023-07-20/'
files = [f for f in glob.glob(path + '*.mrk', recursive=True)]

result = set()
for file_path in tqdm(files):
    # file_path = files[-1]
    marc_list = io.open(file_path, 'rt', encoding = 'utf-8').read().splitlines()
    
    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
                
    for sublist in mrk_list:
        polish_literature = [e for e in sublist if e.startswith('=386') and '$aLiteratura polska' in e]
        if polish_literature:
            polish_literature = True
        else:
            polish_literature = False
        discipline = [e for e in sublist if e.startswith('=386') and '=658' in e]
        if discipline:
            test = False
        else: True
        for el in sublist:
            if polish_literature and el.startswith('=650') and test:
                result.add(marc_parser_to_dict(el, '\\$').get('$a'))
                
                
#naprawić filtrowanie podmiotowej                
#zliczyć wystąpienia motywów
                
motives = [e for e in result if e]
                
                
                
[[el for el in e] for e in mrk_list if [el for el in e if '$aLiteratura polska' in el]]
                
[e for e in marc_list if '$aLiteratura polska' in e]

marc_parser_to_dict('=650  \\7$aKsięgowość$2DBN', '\\$').get('$a')



for file_path in tqdm(files):
    path_mrk = file_path.replace('.mrc', '.mrk')
    mrc_to_mrk(file_path, path_mrk)

path = 'F:/Cezary/Documents/IBL/Migracja z BN/bn_all/2021-02-08/'
path = 'F:/Cezary/Documents/IBL/Translations/Czech database/nkc_SKC_2021-08-05'
path = 'C:/Users/User/Documents/bn_all/2021-07-26/'
files = [file for file in glob.glob(path + '*.mrk', recursive=True)]

conditions = ['$aPogonowska, Anna$d(1922-2005)', '$aOleska, Lucyna', '$aPogonowska, Anna$d1922-2005', '$aPogonowska, Anna$d1922-']

encoding = 'utf-8'
new_list = []
for file_path in tqdm(files):
    marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()

    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
                
    for sublist in mrk_list:
        for el in sublist:
            if el.startswith('=490'):
                if '$aBiblioteka Młodych' in el:
                    new_list.append(sublist)
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    