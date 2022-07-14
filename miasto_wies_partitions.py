import requests
from tqdm import tqdm
import numpy as np
import wikipedia
from my_functions import gsheet_to_df, simplify_string
from concurrent.futures import ThreadPoolExecutor
import json
import glob
from geonames_accounts import geonames_users
import random
import time
from datetime import datetime
from partitions_status import check_place_status, check_if_poland
from math import cos, asin, sqrt
import sys

#%% def

def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    hav = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p)*cos(lat2*p) * (1-cos((lon2-lon1)*p)) / 2
    return 12742 * asin(sqrt(hav))

def closest(data, v):
    return min(data, key=lambda p: distance(v['lat'],v['lon'],p['lat'],p['lon']))

#%% main

path = r'C:\Users\Cezary\Documents\miasto-wies\korpus_1000_final/'
files = [f.split('\\')[-1] for f in glob.glob(path + '*.alt', recursive=True)]

for file in tqdm(files):
    # file=files[0]
    # file = supplement_ids[0] + '.iob.json.polem.alt'
    file_name = file.split('.')[0]
    with open(f'C:/Users/Cezary/Documents/miasto-wies/korpus_1000_final/{file}', encoding='utf8') as f:
        test_file = json.load(f)
    
    try:
        geo_with_one_id_from_poland = [e for e in test_file if len(e['geonames']) == 1 and check_if_poland(float(e['geonames'][0][-2]), float(e['geonames'][0][2]))]
        centre_point = {'lat': sum([float(e['geonames'][0][2]) for e in geo_with_one_id_from_poland])/len(geo_with_one_id_from_poland), 'lon': sum([float(e['geonames'][0][-2]) for e in geo_with_one_id_from_poland])/len(geo_with_one_id_from_poland)}
        not_abort = True
    except KeyboardInterrupt:
        sys.exit()
    except Exception:
        not_abort = False
    # for i, g in enumerate(geo_with_one_id):
    for i, g in enumerate(test_file):
        # i = 0
        # g = test_file[i]
        if len(g['geonames']) == 1:
            test_file[i]['zabór'] = check_place_status(float(g['geonames'][0][-2]), float(g['geonames'][0][2]))
        else:
            if not_abort:
                tempDataList = [{'geonames': e[0], 'lat': float(e[2]), 'lon': float(e[-2])} for e in g['geonames']]
                correct_geonames_id = closest(tempDataList, centre_point)['geonames']
                test_file[i]['geonames'] = [e for e in g['geonames'] if e[0] == correct_geonames_id]
                test_file[i]['zabór'] = check_place_status(float(test_file[i]['geonames'][0][-2]), float(test_file[i]['geonames'][0][2]))
            else: test_file[i]['zabór'] = 'Zagranica'
    with open(f"C:/Users/Cezary/Documents/miasto-wies/korpus_1000_z_zaborami/{file}", 'w', encoding='utf-8') as file:
        json.dump(test_file, file)
            
            






          
    
            
    
    
    
    
    
    
    
