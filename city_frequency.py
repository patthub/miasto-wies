import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
import requests
from tqdm import tqdm
import numpy as np
from my_functions import gsheet_to_df, simplify_string
from concurrent.futures import ThreadPoolExecutor
import json
import glob
from geonames_accounts import geonames_users
import random
import time
from datetime import datetime

#%%

path = r'data/korpus_1000_z_zaborami/'
files = [f.split('\\')[-1] for f in glob.glob(path + '*.alt', recursive=True)]

city_count_total = {}
city_count_per_book = {}
city_in_books = {}
for file in tqdm(files):
    # file=files[0]
    file_name = file.split('.')[0]
    city_count_per_book.update({file_name: dict()})
    with open(f'{path}/{file}', encoding='utf8') as f:
        book_file = json.load(f)
        for entity in book_file:
            # entity = book_file[0]
            city_name = entity.get('geonames')[0][0]
            # city_name = entity.get('geonames')[0][1]
            
            if city_name not in city_count_total:
                city_count_total[city_name] = 1
            else: city_count_total[city_name] += 1
            
            if city_name not in city_count_per_book[file_name]:
                city_count_per_book[file_name][city_name] = 1
            else: city_count_per_book[file_name][city_name] += 1
            
            city_in_books.setdefault(city_name, set()).add(file_name)
             
            
            
            
            
        