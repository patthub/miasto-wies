import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
import requests
from tqdm import tqdm
import numpy as np
from my_functions import gsheet_to_df, simplify_string, cluster_strings
from concurrent.futures import ThreadPoolExecutor
import json
import glob
from geonames_accounts import geonames_users
import random
import time
from datetime import datetime
import Levenshtein as lev

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
            city_name = (entity.get('geonames')[0][0], entity.get('geonames')[0][1])
            if city_name in [(3089423, 'Paryz'), (12036255, 'Paryż')]:
                city_name = (2988507, 'Paryż')
            elif city_name in [(759712, 'Rzym'), (3086390, 'Rzym')]:
                city_name = (3169070, 'Rzym')
            elif city_name in [(761327, 'Poznań'), (3088171, 'Poznan'), (696381, 'Poznan')]:
                city_name = (3088171, 'Poznań')
            elif city_name in [(3082455, 'Wenecja'), (3082456, 'Wenecja')]:
                city_name = (3164603, 'Wenecja')
            elif city_name in [(761318, 'Praga'), (761319, 'Praga'), (3088137, 'Praga'), (3058046, 'Praha'), (3067696, 'Prague')]:
                city_name = (3067696, 'Praga')
            elif city_name in [(755032, 'Wólka'), (11593162, 'Wólka'), (755018, 'Wólka')]:
                city_name = (755032, 'Wólka')
            elif city_name in [(745026, 'Nicaea'), (2990440, 'Nice')]:
                city_name = (2990440, 'Nice')
            # city_name = entity.get('geonames')[0][1]
            
            if city_name not in city_count_total:
                city_count_total[city_name] = 1
            else: city_count_total[city_name] += 1
            
            if city_name not in city_count_per_book[file_name]:
                city_count_per_book[file_name][city_name] = 1
            else: city_count_per_book[file_name][city_name] += 1
            
            city_in_books.setdefault(city_name, set()).add(file_name)
             
            
city_sorted = dict(sorted(city_count_total.items(), key=lambda item: item[1], reverse=True))

cities_top = {k[-1] for k,v in city_sorted.items() if v > 200}
cities_all = {k[-1] for k,v in city_sorted.items()}
cities_rest = [e for e in cities_all if e not in cities_top]

cities_dict = {}
for e in tqdm(cities_top):
    for el in cities_rest:
        if lev.ratio(e, el) > 0.65:
            cities_dict.setdefault(e, []).append(el)


for k,v in city_sorted.items():
    print(v)

test = cluster_strings(cities, 0.8)



#Nicea
#Poznan
#Praha
#Prague

{k:v for k,v in city_count_total.items() if k[-1] == 'Nice'}












            
            
        