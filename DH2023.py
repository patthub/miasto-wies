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
import json
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, FOAF, XSD, OWL
from glob import glob
from datetime import datetime


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

with open('miejsca_urodzenia_zabory.json', 'r', encoding='utf-8') as f:
    miejsca_urodzenia_zabory = json.load(f)
    
# ws_ids = pd.read_csv(r'C:/Users/Cezary/Downloads/wl-index.tsv', sep='\t', header=None)
# ws_ids = dict(zip(ws_ids[0].to_list(), ws_ids[1].to_list()))

ws_ids = {'74179': 'https://pl.wikisource.org/wiki/Ich_dziecko',
 '85934': 'https://pl.wikisource.org/wiki/Argonauci',
 '156206': 'https://pl.wikisource.org/wiki/Anastazya',
 '156637': 'https://pl.wikisource.org/wiki/Pami%C4%99tnik_Wac%C5%82awy',
 '157130': 'https://pl.wikisource.org/wiki/Ostatnia_mi%C5%82o%C5%9B%C4%87',
 '222006': 'https://pl.wikisource.org/wiki/Zygmunt_%C5%81awicz_i_jego_koledzy',
 '286417': 'https://pl.wikisource.org/wiki/Ho%C5%82ota',
 '326241': 'https://pl.wikisource.org/wiki/Wiry',
 '327140': 'https://pl.wikisource.org/wiki/Pi%C4%99kna_pani',
 '327825': 'https://pl.wikisource.org/wiki/Pa%C5%82ac_i_folwark',
 '339193': 'https://pl.wikisource.org/wiki/Inspektor_Mruczek',
 '359263': 'https://pl.wikisource.org/wiki/Za_b%C5%82%C4%99kitami_(Weyssenhoff,_1903)',
 '359335': 'https://pl.wikisource.org/wiki/Nowy_obywatel',
 '362707': 'https://pl.wikisource.org/wiki/Na_cmentarzu,_na_wulkanie',
 '366921': 'https://pl.wikisource.org/wiki/As_(Dygasi%C5%84ski)',
 '367765': 'https://pl.wikisource.org/wiki/Dajmon',
 '372115': 'https://pl.wikisource.org/wiki/Zamorski_djabe%C5%82',
 '372407': 'https://pl.wikisource.org/wiki/Dary_wiatru_p%C3%B3%C5%82nocnego',
 '374788': 'https://pl.wikisource.org/wiki/Pan_Major',
 '378671': 'https://pl.wikisource.org/wiki/Jutro_(Strug,_1911)',
 '398085': 'https://pl.wikisource.org/wiki/Ludzie_elektryczni',
 '398817': 'https://pl.wikisource.org/wiki/W_starym_piecu',
 '402576': 'https://pl.wikisource.org/wiki/Lenin',
 '417771': 'https://pl.wikisource.org/wiki/Wsp%C3%B3lny_pok%C3%B3j',
 '427066': 'https://pl.wikisource.org/wiki/Rajski_ptak_(Bandrowski)',
 '443771': 'https://pl.wikisource.org/wiki/S%C5%82o%C5%84_Birara',
 '466801': 'https://pl.wikisource.org/wiki/Noc_majowa_(Kraszewski,_1884)',
 '466928': 'https://pl.wikisource.org/wiki/Roboty_i_prace',
 '517282': 'https://pl.wikisource.org/wiki/Emisarjusz',
 '576679': 'https://pl.wikisource.org/wiki/Pier%C5%9Bcie%C5%84_z_Krwawnikiem',
 '589580': 'https://pl.wikisource.org/wiki/Moskal',
 '599027': 'https://pl.wikisource.org/wiki/Rodze%C5%84stwo_(Kraszewski,_1884)',
 '605247': 'https://pl.wikisource.org/wiki/Przygody_Jurka_w_Afryce',
 '744856': 'https://pl.wikisource.org/wiki/Sza%C5%82awi%C5%82a'}


content_urls = pd.read_csv(r'C:/Users/Cezary/Downloads/content_urls.tsv', sep='\t', header=None)
content_urls = dict(zip(content_urls[0].to_list(), content_urls[1].to_list()))
content_urls = {f'pl-{k.split("/")[-1]}' if 'polona' in k else f'wl-{k.split("/")[-1]}' if 'wolnelektury' in k else k:v for k,v in content_urls.items()}

ws_correction1 = ['https://pl.wikisource.org/wiki/Rajski_ptak_(Bandrowski)/Całość',
'https://pl.wikisource.org/wiki/As_(Dygasi%C5%84ski)/Całość',
'https://pl.wikisource.org/wiki/Nowy_obywatel/Całość',
'https://pl.wikisource.org/wiki/Ludzie_elektryczni/Całość',
'https://pl.wikisource.org/wiki/Dajmon/Całość',
'https://pl.wikisource.org/wiki/Emisarjusz/Całość',
'https://pl.wikisource.org/wiki/Moskal/Całość',
'https://pl.wikisource.org/wiki/Na_cmentarzu,_na_wulkanie/Całość',
'https://pl.wikisource.org/wiki/Noc_majowa_(Kraszewski,_1884)/Całość',
'https://pl.wikisource.org/wiki/Pa%C5%82ac_i_folwark/Całość',
'https://pl.wikisource.org/wiki/Pan_Major/Całość',
'https://pl.wikisource.org/wiki/Pi%C4%99kna_pani/Całość',
'https://pl.wikisource.org/wiki/Roboty_i_prace/Całość',
'https://pl.wikisource.org/wiki/Rodze%C5%84stwo_(Kraszewski,_1884)/Całość',
'https://pl.wikisource.org/wiki/Sza%C5%82awi%C5%82a/Całość',
'https://pl.wikisource.org/wiki/W_starym_piecu/Całość',
'https://pl.wikisource.org/wiki/Argonauci/Całość',
'https://pl.wikisource.org/wiki/Pami%C4%99tnik_Wac%C5%82awy/Całość',
'https://pl.wikisource.org/wiki/Lenin/Całość',
'https://pl.wikisource.org/wiki/Pier%C5%9Bcie%C5%84_z_Krwawnikiem/Całość',
'https://pl.wikisource.org/wiki/Przygody_Jurka_w_Afryce/Całość',
'https://pl.wikisource.org/wiki/S%C5%82o%C5%84_Birara/Całość',
'https://pl.wikisource.org/wiki/Inspektor_Mruczek/Całość',
'https://pl.wikisource.org/wiki/Wiry/Całość',
'https://pl.wikisource.org/wiki/Dary_wiatru_p%C3%B3%C5%82nocnego/Całość',
'https://pl.wikisource.org/wiki/Zamorski_djabe%C5%82/Całość',
'https://pl.wikisource.org/wiki/Jutro_(Strug,_1911)/Całość',
'https://pl.wikisource.org/wiki/Wsp%C3%B3lny_pok%C3%B3j/Całość',
'https://pl.wikisource.org/wiki/Za_b%C5%82%C4%99kitami_(Weyssenhoff,_1903)/Całość']

ws_correction2 = ['https://pl.wikisource.org/wiki/Na_cmentarzu,_na_wulkanie/całość',
'https://pl.wikisource.org/wiki/Pi%C4%99kna_pani/całość',
'https://pl.wikisource.org/wiki/Dary_wiatru_p%C3%B3%C5%82nocnego/całość',
'https://pl.wikisource.org/wiki/Jutro_(Strug,_1911)/całość']

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
books['wlId'] = books['pierwodruki_id'].apply(lambda x: x.replace('wl-', '') if isinstance(x,str) and x.startswith('wl-') else None)
books['wsId'] = books['pierwodruki_id'].apply(lambda x: x.replace('ws-', '') if isinstance(x,str) and x.startswith('ws-') else None)
books['download'] = books['pierwodruki_id'].apply(lambda x: content_urls.get(x) if x in content_urls else f"{ws_ids.get(x.replace('ws-',''))}/Całość" if not isinstance(x, float) and 'ws' in x else x)
books['download'] = books['download'].apply(lambda x: x.replace('/Całość', '/całość') if x in ws_correction1 else x)
books['download'] = books['download'].apply(lambda x: x.replace('/całość', '') if x in ws_correction2 else x)
books['download'] = books['download'].apply(lambda x: f'https://distantreading.github.io/ELTeC/pol/{x}.html' if isinstance(x,float) else x)

books.drop(columns='pierwodruki_id', inplace=True)

books_json = books.to_dict(orient='index')
books_json = {k:{ka:[e.get('geonameId') for e in literal_eval(va)] if ka == 'geonames' else va for ka,va in v.items()} for k,v in books_json.items()}

people = df[['creator', 'gender', 'creator_wikidata', 'birthplace_id']].drop_duplicates().reset_index(drop=True).rename(columns={'creator': 'name', 'creator_wikidata': 'wikidata', 'birthplace_id': 'birthplace'})

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
places['partition'] = places[['id', 'partition']].apply(lambda x: miejsca_urodzenia_zabory.get(x['id']) if x['partition'] is None else x['partition'], axis=1)

places_json = places.to_dict(orient='index')
places_json = {k:{ka:int(va) if ka == 'geonameId' and va else va for ka,va in v.items()} for k,v in places_json.items()}

#łączenie zbiorów
places_with_geonames = {v.get('geonameId'):k for k,v in places_json.items() if v.get('geonameId')}
people_to_join = {v.get('name'):k for k,v in people_json.items()}

books_json = {k:{ka:people_to_join.get(va) if ka == 'creator' else va for ka,va in v.items()} for k,v in books_json.items()}
for k,v in books_json.items():
    v.update({'publishing place': [places_with_geonames.get(e) for e in v.get('geonames')]})
    
books_json = {k:{ka:va for ka,va in v.items() if ka != 'geonames'} for k,v in books_json.items()}


partition_dict = {'metapnc_z_1569': {'name': 'Austrian Partition',
                                     'wikidata': 'https://www.wikidata.org/wiki/Q129794', 
                                     'id': 'metapnc_z_1569'},
                  'metapnc_z_1570': {'name': 'Prussian Partition',
                                     'wikidata': 'https://www.wikidata.org/wiki/Q129791', 
                                     'id': 'metapnc_z_1570'},
                  'metapnc_z_1571': {'name': 'Russian Partition',
                                     'wikidata': 'https://www.wikidata.org/wiki/Q129797', 
                                     'id': 'metapnc_z_1571'}}

literary_epochs = {'metapnc_e_1572': {'name': 'Interwar period', 
                                      'wikidata': 'https://www.wikidata.org/wiki/Q11761904',
                                      'id': 'metapnc_e_1572'},
                   'metapnc_e_1573': {'name': 'Young Poland',
                                      'wikidata': 'https://www.wikidata.org/wiki/Q1133329',
                                      'id': 'metapnc_e_1573'},
                   'metapnc_e_1574': {'name': 'Positivism ',
                                      'wikidata': 'https://www.wikidata.org/wiki/Q131015',
                                      'id': 'metapnc_e_1574'}}

places_wikidata = {v.get('wikidataId'):k for k,v in places_json.items() if v.get('wikidataId')}
people_json = {k:{ka:places_wikidata.get(va) if ka == 'birthplace' and va in places_wikidata else va for ka,va in v.items()} for k,v in people_json.items()}

jsons = {'dh2023_books': books_json,
         'dh2023_people': people_json,
         'dh2023_places': places_json,
         'dh2023_epochs': literary_epochs,
         'dh2023_partitions': partition_dict}

for k, v in jsons.items():
    with open(f'{k}.json', 'w') as f:
        json.dump(v, f, ensure_ascii=False)

#%% ontologia
#load json
files = [f for f in glob('dh*.json', recursive=True)]
files_dict = {}
for file in files:
    name = file.replace('.json', '')
    with open(file, 'r') as f:
        files_dict[name] = json.load(f) 
#namespaces
TCO = Namespace("http://purl.org/computations/tco/")
n = Namespace("http://example.org/people/")
dcterms = Namespace("http://purl.org/dc/terms/")
rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
book_uri = "http://miastowies.org/item/"
eltec_uri = "http://distantreading.github.io/ELTeC/pol/"
polona_uri = "http://polona.pl/item/"
wl_uri = "https://wolnelektury.pl/katalog/lektura/"
ws_uri = "https://pl.wikisource.org/wiki/"
FABIO = Namespace("http://purl.org/spar/fabio/")
BIRO = Namespace("http://purl.org/spar/biro/")
VIAF = Namespace("http://viaf.org/viaf/")
WIKIDATA = Namespace("http://www.wikidata.org/entity/")
GEONAMES = Namespace("https://www.geonames.org/")
geo = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
bibo = Namespace("http://purl.org/ontology/bibo/")
schema = Namespace("http://schema.org/")
#def
def add_partition(partition_dict):

    partition = URIRef(TCO + partition_dict['id'])
    g.add((partition, RDF.type, dcterms.Location))
    g.add((partition, RDFS.label, Literal(partition_dict["name"])))
    g.add((partition, TCO.isPartition, Literal(True)))
    g.add((partition, OWL.sameAs, URIRef(partition_dict["wikidata"])))


def add_epoch(epoch_dict):

    epoch = URIRef(TCO + epoch_dict['id'])
    g.add((epoch, RDF.type, dcterms.PeriodOfTime))
    g.add((epoch, RDFS.label, Literal(epoch_dict["name"])))
    g.add((epoch, TCO.isEpoch, Literal(True)))
    g.add((epoch, OWL.sameAs, URIRef(epoch_dict["wikidata"])))

def add_place(place_dict):

    place = URIRef(TCO + place_dict['id'])
    
    
    g.add((place, RDF.type, dcterms.Location))
    g.add((place, RDFS.label, Literal(place_dict["name"])))
    
    ##POINT
    
    # Add the triple for latitude
    latitude = Literal(place_dict["lat"], datatype=XSD.float)
    g.add((place, geo.lat, latitude))
    
    # Add the triple for longitude
    longitude = Literal(place_dict["lng"], datatype=XSD.float)
    g.add((place, geo.long, longitude))
    
    ##/POINT
    if place_dict["wikidataId"]:
        g.add((place, OWL.sameAs, URIRef(WIKIDATA+place_dict["wikidataId"])))
    if place_dict['geonameId']:
        g.add((place, OWL.sameAs, URIRef(GEONAMES+str(place_dict["geonameId"]))))
    if place_dict['partition']:
        g.add((place, TCO.inPartition, URIRef(TCO+place_dict["partition"])))

def add_book(book_dict):

    book = URIRef(TCO + book_dict['id'])
    
    g.add((corpus, TCO.contains, book))
    g.add((book, RDF.type, TCO.Text))
    g.add((book, RDF.type, dcterms.BibliographicResource))
    g.add((book, dcterms.title, Literal(book_dict["title"])))
    g.add((book, dcterms.creator, URIRef(TCO + book_dict["creator"])))
    g.add((book, dcterms.date, Literal(book_dict["year"], datatype = XSD.year)))
    if book_dict["ELTeC"] and not isinstance(book_dict["ELTeC"], float):
        g.add((book, OWL.sameAs, URIRef(eltec_uri + book_dict["ELTeC"])))
    if book_dict["polonaId"]:
        g.add((book, OWL.sameAs, URIRef(polona_uri + book_dict["polonaId"])))
    if book_dict["wlId"]:
        g.add((book, OWL.sameAs, URIRef(wl_uri + book_dict["wlId"])))
    if book_dict["wsId"] and not isinstance(book_dict["wsId"], float):
        g.add((book, OWL.sameAs, URIRef(ws_ids[book_dict["wsId"]])))
    # g.add((book, TCO.inEpoch, URIRef(TCO + "epoch/" + book_dict["epoka"])))
    g.add((book, TCO.inEpoch, URIRef(TCO + book_dict["epoka"])))
    g.add((book, TCO.numberOfReissues, Literal(book_dict["liczba wznowień"], datatype = XSD.integer)))
    g.add((book, TCO.numberOfTokens, Literal(book_dict["num_tokens"], datatype = XSD.integer)))
    for place in book_dict["publishing place"]:
        g.add((book, FABIO.hasPlaceOfPublication, URIRef(TCO + place)))
    g.add((book, schema.genre, Literal('Novel')))
    g.add((book, dcterms.subject, Literal('Plot after the Congress of Vienna')))
    g.add((book, schema.contentUrl, URIRef(book_dict['download'])))
  
    
def add_person(person_dict):

    person = URIRef(TCO + person_dict['id'])
    
    #g.add((corpus, TCO.?, book) co robi korpus?
    g.add((person, RDF.type, FOAF.Person))
    g.add((person, FOAF.gender, Literal(person_dict["gender"])))
    g.add((person, RDFS.label, Literal(person_dict["name"])))
    if person_dict["wikidata"] and not isinstance(person_dict["wikidata"], float):
        g.add((person, OWL.sameAs, URIRef(person_dict["wikidata"])))
    if person_dict['birthplace'] and not isinstance(person_dict["birthplace"], float):
        g.add((person, schema.birthPlace, URIRef(TCO+person_dict["birthplace"])))
  
#graph

# Graph instantiation
g = Graph()

g.bind("tco", TCO)
g.bind("dcterms", dcterms)
g.bind("fabio", FABIO)
g.bind("geo", geo)
g.bind("bibo", bibo)
g.bind("schema", schema)
g.bind("biro", BIRO)

# Create corpora node
corpus = URIRef(TCO + "Corpora")
g.add((corpus, RDF.type, TCO.Corpus))
g.add((corpus, schema.provider, Literal("Instytut Badań Literackich PAN")))
g.add((corpus, dcterms.license, URIRef('https://creativecommons.org/licenses/by/4.0/')))
g.add((corpus, dcterms.created, Literal(datetime.today(), datatype=XSD.dateTime)))


for k,v in tqdm(files_dict.items()):
    if k == 'dh2023_partitions':
        for ka,va in v.items():
            add_partition(va)
    elif k == 'dh2023_epochs':
        for ka,va in v.items():
            add_epoch(va)
    elif k == 'dh2023_places':
        for ka,va in v.items():
            add_place(va)
    elif k == 'dh2023_people':
        for ka,va in v.items():
            add_person(va)
    elif k == 'dh2023_books':
        for ka,va in v.items():
            add_book(va)

        
# for k,v in partition_dict.items():
#     add_partition(v)
# for k,v in literary_epochs.items():
#     add_epoch(v)
# for k,v in places_json.items():
#     add_place(v)
# for k,v in books_json.items():
#     add_book(v)
# for k,v in people_json.items():
#     add_person(v)

# print(g.serialize(format='xml'))

g.serialize("metapnc.ttl", format = "turtle")
































