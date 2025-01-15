import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import gsheet_to_df, simplify_string, cluster_strings
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

#%%
df = gsheet_to_df('1yefQ2CKTWFW4LcIIilxpflbn9Ic8QFe72hRfb50MMoo', 'statystyki_2')

ids = df['id'].to_list()

url = 'https://wolnelektury.pl/katalog/lektura/'

url = 'https://wolnelektury.pl/katalog/lektura/tako-rzecze-zaratustra'
html_text = requests.get(url).text
soup = BeautifulSoup(html_text, "html.parser")
wikipedia_url = soup.find('a', string='strona utworu w Wikipedii')['href']

link = 'https://pl.wikipedia.org/w/api.php?action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&format=json&titles=Tako_rzecze_Zaratustra'

result = requests.get(link).json()
wikipedia_id = list(result.get('query').get('pages').keys())[0]
wikidata_id = result.get('query').get('pages').get(wikipedia_id).get('pageprops').get('wikibase_item')
title = result.get('query').get('pages').get(wikipedia_id).get('title')


r = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()

inception = r.get('entities').get(wikidata_id).get('claims').get('P571')

record_languages = set(r.get('entities').get(wikidata_id).get('labels').keys())
for language in list_of_languages:
    if language in record_languages:
        return r.get('entities').get(wikidata_id).get('labels').get(language).get('value')
    else:
        return r.get('entities').get(wikidata_id).get('labels').get(list(record_languages)[0]).get('value')
















