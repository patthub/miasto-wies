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

#%%

# rejestry_1 = gsheet_to_df('1mvLpNToF1vvMybaVfO_Ghh7qtdhP7zzXjznQGh4eyTk', 'Krzysztofik_ miasta do 1939')
# rejestry_2 = gsheet_to_df('1mvLpNToF1vvMybaVfO_Ghh7qtdhP7zzXjznQGh4eyTk', 'Najgrakowski_miasta do 1939')

# # uproszczenie nazw zrobić już na tym poziomie

# rejestry_1_dict = dict(zip(rejestry_1['Nazwa obecna miasta'].map(lambda x: x.lower()), [[f.strip().lower() for f in e.split(';')] if not isinstance(e, float) else e for e in rejestry_1['Nazwy historyczne oddzielone średnikami'].to_list()]))

# rejestry_2_dict = dict(zip(rejestry_2['Obecna nazwa'].map(lambda x: simplify_string(x, nodiacritics=False)), [[f.strip().lower() for f in e.split(',')] if not isinstance(e, float) else e for e in rejestry_2['Warianty oddzielone przecinkami'].to_list()]))


# rejestry = {}
# w_1_i_w_2 = []
# for k,v in tqdm(rejestry_1_dict.items()):
#     # k = "BARCIANY"
#     # v = rejestry_1_dict[k]
#     k = simplify_string(k, nodiacritics=False)
#     temp_dict = {ka:va for ka,va in rejestry_2_dict.items() if k == ka}
#     v1 = v if isinstance(v, list) else []
#     try:
#         v2 = temp_dict[k] if isinstance(temp_dict[k], list) else []
#         value =  v1 + v2 
#         value = set(value)
#         w_1_i_w_2.append(k)
#     except KeyError:
#         value = set(v1)
#     rejestry[k] = value
    
# tylko_w_2 = set(rejestry_2_dict.keys()) - set(w_1_i_w_2)
# dodatek_z_2 = {k:set(v) if isinstance(v,list) else {} for k,v in rejestry_2_dict.items() if k in tylko_w_2}

# rejestry.update(dodatek_z_2)
   
#co, jeśli coś występuje w 1, a nie ma w 2 – try except

#co, jeśli coś występuje w 2, a nie ma w 1 – trzeba zbierać klucze, które zostaną znalezione, a następnie wybrać te, które nie zostały znalezione i uzupełnić zmienną rejestry


# url = 'http://api.geonames.org/searchJSON?'
# params = {'username': 'crosinski', 'q': 'Upita', 'featureClass': 'P', 'style': 'FULL'}
# result = requests.get(url, params=params).json()

# test = [(e['name'], e['lat'], e['lng'], [f['name'] for f in e['alternateNames'] if f['lang'] != 'link']) for e in result['geonames']]

#%% download NLP file
path = r'C:\Users\Cezary\Documents\miasto-wies\korpus_1000/'
path = r'C:\Users\Cezary\Documents\miasto-wies\korpus_1000_uzupełnienia/'
files = [f.split('\\')[-1] for f in glob.glob(path + '*.polem', recursive=True)]
files = [f.split('\\')[-1] for f in glob.glob(path + '*.alt', recursive=True)]

files_dict = {}
for file in tqdm(files):
    # file=files[0]
    file_name = file.split('.')[0]
    # with open(f'C:/Users/Cezary/Documents/miasto-wies/korpus_1000/{file}', encoding='utf8') as f:
    #     test_file = json.load(f)
    with open(f'C:/Users/Cezary/Documents/miasto-wies/korpus_1000_uzupełnienia/{file}', encoding='utf8') as f:
        test_file = json.load(f)
    test_file = [e for e in test_file if e['type'] in ['nam_loc_gpe_city', 'nam_loc']]
    # origin_set = [' '.join(e['lemmas']) for e in test_file]
    # simple_set = list(set([' '.join([f.replace('.','').lower().strip('-').strip('–').strip(': ').strip('+').strip(', ').strip('.').strip() for f in e['lemmas']]) for e in test_file]))
    # origin_set = [e['polem'] for e in test_file]
    
    origin_set = [e['polem'] if any(f in e['polem'] for f in [' ']) else e['lemmas'][0] for e in test_file]
    simple_set = [e.lower() for e in origin_set]
    files_dict[file_name] = {'origin': origin_set,
                             'simple': simple_set}

with open(f'miejscowosci_dict_uzupełnienie_{datetime.now().date()}.json', 'w') as f:
    json.dump(files_dict, f)

unique_geo_entities = [set(files_dict[e]['simple']) for e in files_dict]
unique_geo_entities = list([e for e in set.union(*unique_geo_entities) if e])
#23794
# n = 10000
# final = [unique_geo_entities[i * n:(i + 1) * n] for i in range((len(unique_geo_entities) + n - 1) // n )]

#%%

with open('miejscowosci_total_filtered_2022-07-14.json', 'r', encoding='utf-8') as f:
    polem_based = json.load(f)
with open('miejscowosci_dict_2022-07-14.json', 'r', encoding='utf-8') as f:
    polem_based_dict = json.load(f)

with open('miejscowosci_total_filtered.json', 'r', encoding='utf-8') as f:
    lemmas_based = json.load(f)
with open('miejscowosci_dict.json', 'r', encoding='utf-8') as f:
    lemmas_based_dict = json.load(f)
    

#%% get main name
# rejestry_full = {k:set(v) for k,v in rejestry_full.items()}

# test1 = set([' '.join([f.replace('.','').lower().strip().strip('-').strip('–') for f in e['lemmas']]) for e in test_file])

# [{k for k,v in rejestry_full.items() if e in v} for e in test1]



#%% geonames query

# miejscowosci = ['Żmudź', 'Wodokty', 'Lubicz', 'Upita', 'Kiejdany', 'Taurogi', 'Adamów', 'Jadaromin']
# miejscowosci = final[1]
# miejscowosci = final[1]
# miejscowosci = final[2]

miejscowosci = unique_geo_entities.copy()

# miejscowosci_total = {}
# for m in tqdm(miejscowosci[19152:19160]):
def query_geonames(m):
    # m = 'Adamów'
    url = 'http://api.geonames.org/searchJSON?'
    params = {'username': random.choice(geonames_users), 'q': m, 'featureClass': 'P', 'style': 'FULL'}
    result = requests.get(url, params=params).json()
    if 'status' in result:
        time.sleep(5)
        query_geonames(m)
    else:
        geonames_resp = [[e['geonameId'], e['name'], e['lat'], e['lng'], [f['name'] for f in e['alternateNames']] if 'alternateNames' in e else []] for e in result['geonames']]
        [e[-1].append(e[1]) for e in geonames_resp]
        if len(geonames_resp) == 0:
            miejscowosci_total[m] = geonames_resp
        if len(geonames_resp) == 1:
            miejscowosci_total[m] = geonames_resp
        elif len(geonames_resp) > 1:    
            for i, resp in enumerate(geonames_resp):
                # i = -1
                # resp = geonames_resp[-1]
                if len(resp[-1]) > 1:
                    try:
                        wikipedia_link = [e for e in resp[-1] if 'wikipedia' in e][0]
                        wikipedia_title = wikipedia_link.replace('https://en.wikipedia.org/wiki/','')
                        wikipedia_query = f'https://en.wikipedia.org/w/api.php?action=query&format=json&prop=pageprops&ppprop=wikibase_item&redirects=1&titles={wikipedia_title}'
                        try:
                            wikipedia_result = requests.get(wikipedia_query).json()['query']['pages']
                            wikidata_id = wikipedia_result[list(wikipedia_result.keys())[0]]['pageprops']['wikibase_item']
                            wikidata_query = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()
                            labels = {v['value'] for k,v in wikidata_query['entities'][wikidata_id]['labels'].items()}
                            geonames_resp[i][-1].extend(labels)
                            geonames_resp[i][-1].remove(wikipedia_link)
                        # except (KeyError, requests.exceptions.ConnectTimeout):
                        except Exception:
                            pass
                    except IndexError:
                        pass
            miejscowosci_total[m] = geonames_resp

miejscowosci_total = {}
with ThreadPoolExecutor() as excecutor:
    list(tqdm(excecutor.map(query_geonames, miejscowosci),total=len(miejscowosci)))
    
with open(f'miejscowosci_total_uzupełnienie_{datetime.now().date()}.json', 'w') as f:
    json.dump(miejscowosci_total, f)
    
# miejscowosci.index(list(miejscowosci_total.keys())[-1])
    
#zapisać każdy plik z osobna na dysku
        
#%% disambiguation
# rejestry_full = rejestry.copy()
# rejestry_full = {k:set(v) for k,v in rejestry_full.items()}
# {k:v.add(k) for k,v in rejestry_full.items()}

miejscowosci_total_filtered = {k:[e for e in v if any(k == f.lower() for f in e[-1])] for k,v in miejscowosci_total.items()}
miejscowosci_total_filtered = {k:v for k,v in miejscowosci_total_filtered.items() if v}

with open(f'miejscowosci_total_filtered_uzupełnienie_{datetime.now().date()}.json', 'w') as f:
    json.dump(miejscowosci_total_filtered, f)

#%% łączenie zbiorów

with open('miejscowosci_total_filtered_2022-07-14.json', 'r', encoding='utf-8') as f:
    polem_based = json.load(f)
with open('miejscowosci_dict_2022-07-14.json', 'r', encoding='utf-8') as f:
    polem_based_dict = json.load(f)

with open('miejscowosci_total_filtered.json', 'r', encoding='utf-8') as f:
    lemmas_based = json.load(f)
with open('miejscowosci_dict.json', 'r', encoding='utf-8') as f:
    lemmas_based_dict = json.load(f)
    
with open('miejscowosci_total_filtered_uzupełnienie_2022-07-14.json', 'r', encoding='utf-8') as f:
    supplement_based = json.load(f)
with open('miejscowosci_dict_uzupełnienie_2022-07-14.json', 'r', encoding='utf-8') as f:
    supplement_based_dict = json.load(f)

supplement_ids = [f.split('\\')[-1] for f in glob.glob(r'C:\Users\Cezary\Documents\miasto-wies\korpus_1000_uzupełnienia/' + '*.alt', recursive=True)]
supplement_ids = [e.split('.')[0] for e in supplement_ids]

path = r'C:\Users\Cezary\Documents\miasto-wies\korpus_1000/'
files = [f.split('\\')[-1] for f in glob.glob(path + '*.alt', recursive=True)]


no_of_ners = 0
no_of_ners_with_geonames = 0
files_dict = {}
for file in tqdm(files):
    # file=files[0]
    # file = supplement_ids[0] + '.iob.json.polem.alt'
    file_name = file.split('.')[0]
    with open(f'C:/Users/Cezary/Documents/miasto-wies/korpus_1000/{file}', encoding='utf8') as f:
        test_file = json.load(f)
    test_file = [e for e in test_file if e['type'] in ['nam_loc_gpe_city', 'nam_loc']]   
    if file_name in supplement_ids:
        test_dict = supplement_based_dict[file_name]
        for i, d in enumerate(test_file):
            # i = 0
            # d = test_file[i]
            test_file[i].update({'geonames': supplement_based[test_dict['simple'][i]] if test_dict['simple'][i] in supplement_based else np.nan})
    else:
        for i, d in enumerate(test_file):
            # i = 65
            # d = test_file[i]
            if ' ' in d['polem']:
                test_dict = polem_based_dict[file_name]
                test_file[i].update({'geonames': polem_based[test_dict['simple'][i]] if test_dict['simple'][i] in polem_based else np.nan})
            else:
                test_dict = lemmas_based_dict[file_name]
                try:
                    test_file[i].update({'geonames': lemmas_based[d['lemmas'][0].replace('.','').lower().strip('-').strip('–').strip(': ').strip('+').strip(', ').strip('.').strip()]})
                except KeyError:
                    test_file[i].update({'geonames': np.nan})
    no_of_ners += len(test_file)
    test_file = [e for e in test_file if not isinstance(e['geonames'], float)]
    no_of_ners_with_geonames += len(test_file)
    with open(f"C:/Users/Cezary/Documents/miasto-wies/korpus_1000_final/{file}", 'w', encoding='utf-8') as file:
        json.dump(test_file, file)

print(no_of_ners)
print(no_of_ners_with_geonames)    
                

with open(r"C:\Users\Cezary\Documents\miasto-wies\korpus_1000_final\pl-10072489.iob.json.polem.alt", 'r', encoding='utf-8') as f:
    test = json.load(f)

            
        
        
        
    
    for d in test_file:
        d = test_file[0]
        if ' ' in d['polem']:
            
            
        
        
        
        
    
    # origin_set = [' '.join(e['lemmas']) for e in test_file]
    # simple_set = list(set([' '.join([f.replace('.','').lower().strip('-').strip('–').strip(': ').strip('+').strip(', ').strip('.').strip() for f in e['lemmas']]) for e in test_file]))
    # origin_set = [e['polem'] for e in test_file]
    
    origin_set = [e['polem'] if any(f in e['polem'] for f in [' ']) else e['lemmas'][0] for e in test_file]
    simple_set = [e.lower() for e in origin_set]
    files_dict[file_name] = {'origin': origin_set,
                             'simple': simple_set}
    



# v = miejscowosci_total['Lubicz']
# [e[-1] for e in v]

# page = 'Upyt%C4%97'
# wikipedia_page = wikipedia.WikipediaPage(page)
# page(title=page).page_id



# with open(r"C:\Users\Cezary\Downloads\pl-6058.ner.json") as f:
#     test = json.load(f)



# nazwy = set([e['text'] for e in test])



































'https://en.wikipedia.org/wiki/Upyt%C4%97'

'https://en.wikipedia.org/w/api.php?action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&titles=ARTICLE_NAME'
test = 'https://en.wikipedia.org/w/api.php?action=query&format=json&prop=pageprops&ppprop=wikibase_item&redirects=1&titles=Upyt%C4%97'
result = requests.get(test).json()['query']['pages']
wikidata_id = result[list(result.keys())[0]]['pageprops']['wikibase_item']

wikidata_query = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()







q_country = ccodes[country.lower()]
params = {'username': 'crosinski', 'q': city, 'featureClass': 'P', 'country': q_country}
result = requests.get(url, params=params).json()
places_geonames_extra_2.update({old: max(result['geonames'], key=lambda x: x['population'])})
except (KeyError, ValueError):
errors.append((old, city, country))