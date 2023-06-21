import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import gsheet_to_df
import pandas as pd
from ast import literal_eval
import json
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.error import HTTPError, URLError
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import requests
import time
import numpy as np

#%% def
def wikidata_simple_dict_resp(results):
    results = results['results']['bindings']
    dd = defaultdict(list)
    for d in results:
        for key, value in d.items():
            dd[key].append(value)
    dd = {k:set([tuple(e.items()) for e in v]) for k,v in dd.items()}
    dd = {k:list([dict((x,y) for x,y in e) for e in v]) for k,v in dd.items()}
    return dd

def query_wikidata_place_with_geoname(geoname_id):
# for geoname_id in tqdm(geoname_ids):
    # geoname_id = 498817
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    sparql.setQuery(f"""SELECT DISTINCT ?item ?itemLabel WHERE {{
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }}
      {{
        SELECT DISTINCT ?item WHERE {{
          ?item p:P1566 ?statement0.
          ?statement0 (ps:P1566) "{geoname_id}".
        }}
      }}
    }}""")
    sparql.setReturnFormat(JSON)
    while True:
        try:
            results = sparql.query().convert()
            break
        except HTTPError:
            time.sleep(2)
        except URLError:
            time.sleep(5)
    results = wikidata_simple_dict_resp(results)
    geo_wiki[geoname_id] = results.get('itemLabel')[0].get('value')
    # return results.get('itemLabel')[0].get('value')

wiki_geo = {}    
def get_wikidata_for_geonameId(wikidata_id):
# for wikidata_id in tqdm(wikidata_ids):
    # wikidata_id = 'Q2280'
    result = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()
    try:
        wiki_geo[wikidata_id] = result.get('entities').get(wikidata_id).get('claims').get('P1566')[0].get('mainsnak').get('datavalue').get('value')
    except TypeError:
        wiki_geo[wikidata_id] = None

#%% load

podkorpus_1000 = gsheet_to_df('17OHdiMqtfzPSzkeyPHcCzdGY8gOhek3bhv1zKjs9CCc', 'Podkorpus 1000')
korpus_roboczy = gsheet_to_df('17OHdiMqtfzPSzkeyPHcCzdGY8gOhek3bhv1zKjs9CCc', 'korpus roboczy')
ostateczna_deduplikacja = gsheet_to_df('17OHdiMqtfzPSzkeyPHcCzdGY8gOhek3bhv1zKjs9CCc', 'ostateczna deduplikacja')


podkorpus_1000 = podkorpus_1000.head(1000).drop(columns=podkorpus_1000.columns.values[-2:]).drop(columns='CZY ISTNIEJE W JEDNYM Z NICH')
ostateczna_deduplikacja = ostateczna_deduplikacja[['lp', 'creator_wikidata', 'geonames', 'num_tokens', 'birthplace', 'birthplace_id', 'birthplace_latitude', 'birthplace_longitude', 'liczba wznowień', 'zabór_full']]

df = pd.merge(podkorpus_1000, ostateczna_deduplikacja, on='lp', how='left')

# korpus_roboczy = korpus_roboczy[['id', 'bn_genre']]

# df = pd.merge(df, korpus_roboczy, left_on='pierwodruki_id', right_on='id', how='left')

#dopytać Agnieszkę --> czy CZY ISTNIEJE W JEDNYM Z NICH jest potrzebne, czy brać coś z korpusu roboczego?

#%% main

books = df[['lp', 'creator', 'title', 'epoka', 'liczba wznowień', 'ELTeC', 'year', 'num_tokens', 'geonames', 'pierwodruki_id']]

length = books.shape[0]
for l in range(1, length+1):
    books.at[l-1, 'id'] = f'metapnc_b_{l}'

books.index = books['id']

literary_epochs = {'DM': 'metapnc_e_1572',
                   'MP': 'metapnc_e_1573',
                   'P': 'metapnc_e_1574'}

books['epoka'] = books['epoka'].apply(lambda x: literary_epochs.get(x))
books['polonaId'] = books['pierwodruki_id'].apply(lambda x: x.replace('pl-', '') if isinstance(x,str) and x.startswith('pl-') else None)
books.drop(columns='pierwodruki_id', inplace=True)

books_json = books.to_dict(orient='index')
books_json = {k:{ka:[e.get('geonameId') for e in literal_eval(va)] if ka == 'geonames' else va for ka,va in v.items()} for k,v in books_json.items()}

people = df[['creator', 'gender', 'creator_wikidata']].drop_duplicates().reset_index(drop=True).rename(columns={'creator': 'name', 'creator_wikidata': 'wikidata'})

length = people.shape[0] + length
for i, row in people.iterrows():
    people.at[i, 'id'] =  f'metapnc_p_{range(1001, length+1)[i]}'

people.index = people['id']

gender_dict = {'M': 'male',
            'K': 'female'}

people['gender'] = people['gender'].apply(lambda x: gender_dict.get(x))

people_json = people.to_dict(orient='index')

test = [dict(ele) for ele in set([tuple(el.items()) for sub in [literal_eval(e) for e in df['geonames'].to_list()] for el in sub])]
test = pd.DataFrame(test)

geo_zabor_dict = {}
for i, row in tqdm(df.iterrows()):
    # i = 0
    # row = df.iloc[0,]
    geonames = literal_eval(row['geonames'])
    partitions = literal_eval(row['zabór_full'])
    for g, p in zip(geonames, partitions):
        if p != 'zagranica':
            geo_zabor_dict.setdefault(g.get('geonameId'), p)

geoname_ids = set(test['geonameId'].to_list())

geo_wiki = {}
# with ThreadPoolExecutor() as executor:
#     list(tqdm(executor.map(query_wikidata_place_with_geoname,geoname_ids), total=len(geoname_ids)))
for geoname_id in tqdm(geoname_ids):
    # geoname_id = 498817
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    sparql.setQuery(f"""SELECT DISTINCT ?item ?itemLabel WHERE {{
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }}
      {{
        SELECT DISTINCT ?item WHERE {{
          ?item p:P1566 ?statement0.
          ?statement0 (ps:P1566) "{geoname_id}".
        }}
      }}
    }}""")
    sparql.setReturnFormat(JSON)
    while True:
        try:
            results = sparql.query().convert()
            break
        except HTTPError:
            time.sleep(2)
        except URLError:
            time.sleep(5)
    results = wikidata_simple_dict_resp(results)
    geo_wiki[geoname_id] = results.get('itemLabel')[0].get('value')
    
test['wikidataId'] = test['geonameId'].apply(lambda x: geo_wiki.get(x))

test2 = df[['birthplace', 'birthplace_id',
'birthplace_latitude', 'birthplace_longitude']].drop_duplicates().dropna().rename(columns={'birthplace': 'name', 'birthplace_id': 'wikidataId', 'birthplace_latitude': 'lat', 'birthplace_longitude': 'lng'})

wikidata_ids = set(test2['wikidataId'].to_list())

wiki_geo = {}
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_wikidata_for_geonameId,wikidata_ids), total=len(wikidata_ids)))
    
test2['geonameId'] = test2['wikidataId'].apply(lambda x: wiki_geo.get(x))

places = pd.concat([test, test2]).reset_index(drop=True)
places = places.groupby('wikidataId').head(1).reset_index(drop=True)
places['partition'] = places['geonameId'].apply(lambda x: geo_zabor_dict.get(x))
places['geonameId'] = places['geonameId'].apply(lambda x: str(x) if x else x)

partition_dict = {'austriacki': 'metapnc_z_1569', 
                  'pruski': 'metapnc_z_1570', 
                  'rosyjski': 'metapnc_z_1571'}

places['partition'] = places['partition'].apply(lambda x: partition_dict.get(x))

length = places.shape[0] + length
for i, row in places.iterrows():
    places.at[i, 'id'] =  f'metapnc_g_{range(1391, length+1)[i]}'

places.index = places['id']

places_json = places.to_dict(orient='index')
places_json = {k:{ka:int(va) if ka == 'geonameId' and va else va for ka,va in v.items()} for k,v in places_json.items()}

#łączenie zbiorów
places_with_geonames = {v.get('geonameId'):k for k,v in places_json.items() if v.get('geonameId')}
people_to_join = {v.get('name'):k for k,v in people_json.items()}

books_json = {k:{ka:people_to_join.get(va) if ka == 'creator' else va for ka,va in v.items()} for k,v in books_json.items()}
for k,v in books_json.items():
    v.update({'publishing place': [places_with_geonames.get(e) for e in v.get('geonames')]})
    
books_json = {k:{ka:va for ka,va in v.items() if ka != 'geonames'} for k,v in books_json.items()}


partition_dict = {'metapnc_z_1569': {'name': 'Zabór austriacki',
                                     'wikidata': 'https://www.wikidata.org/wiki/Q129794', 
                                     'id': 'metapnc_z_1569'},
                  'metapnc_z_1570': {'name': 'Zabór pruski',
                                     'wikidata': 'https://www.wikidata.org/wiki/Q129791', 
                                     'id': 'metapnc_z_1570'},
                  'metapnc_z_1571': {'name': 'Zabór rosyjski',
                                     'wikidata': 'https://www.wikidata.org/wiki/Q129797', 
                                     'id': 'metapnc_z_1571'}}

literary_epochs = {'metapnc_e_1572': {'name': 'Dwudziestolecie międzywojenne', 
                                      'wikidata': 'https://www.wikidata.org/wiki/Q11761904',
                                      'id': 'metapnc_e_1572'},
                   'metapnc_e_1573': {'name': 'Młoda Polska',
                                      'wikidata': 'https://www.wikidata.org/wiki/Q1133329',
                                      'id': 'metapnc_e_1573'},
                   'metapnc_e_1574': {'name': 'Pozytywizm',
                                      'wikidata': 'https://www.wikidata.org/wiki/Q131015',
                                      'id': 'metapnc_e_1574'}}

jsons = {'dh2023_books': books_json,
         'dh2023_people': people_json,
         'dh2023_places': places_json,
         'dh2023_epochs': literary_epochs,
         'dh2023_partitions': partition_dict}

for k, v in jsons.items():
    with open(f'{k}.json', 'w') as f:
        json.dump(v, f, ensure_ascii=False)

#%% next
#mapowanie na ontologię --> z PH
ontology_columns = {'lp', 
                    'creator', 
                    'title', 
                    'gender', 
                    'zabór', 
                    'epoka', 
                    'recepcja',
                    'ELTeC', 
                    'pierwodruki_id', 
                    'year', 
                    'corpus_id', 
                    'creator_wikidata',
                    'geonames', 
                    'num_tokens', 
                    'birthplace', 
                    'birthplace_id',
                    'birthplace_latitude', 
                    'birthplace_longitude'}