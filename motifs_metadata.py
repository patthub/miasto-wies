import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import gsheet_to_df, simplify_string, cluster_strings
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

#%%
df = gsheet_to_df('1yefQ2CKTWFW4LcIIilxpflbn9Ic8QFe72hRfb50MMoo', 'statystyki_2')

ids = df['id'].to_list()

wl_url = 'https://wolnelektury.pl/katalog/lektura/'

results_list = []
wl_errors = []
wl_without_wiki = []
wl_wiki = {}
for text_id in tqdm(ids):
    # text_id = ids[0]

    url = f'https://wolnelektury.pl/katalog/lektura/{text_id}'
    resp = requests.get(url)
    if resp.status_code == 404:
        wl_errors.append(text_id)
    else:
        html_text = requests.get(url).text
        soup = BeautifulSoup(html_text, "html.parser")
        try:
            wikipedia_url = soup.find('a', string='strona utworu w Wikipedii')['href']
            wl_wiki.update({text_id: wikipedia_url})
        except TypeError:
            wl_without_wiki.append(text_id)
        
        
    wikipedia_title = wikipedia_url.split('/')[-1]

    link = f'https://pl.wikipedia.org/w/api.php?action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&format=json&titles={wikipedia_title}'

    result = requests.get(link).json()
    wikipedia_id = list(result.get('query').get('pages').keys())[0]
    wikidata_id = result.get('query').get('pages').get(wikipedia_id).get('pageprops').get('wikibase_item')
    title = result.get('query').get('pages').get(wikipedia_id).get('title')

    r = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()

    try:
        inception = r.get('entities').get(wikidata_id).get('claims').get('P571')[0].get('mainsnak').get('datavalue').get('value').get('time')[1:5]
    except TypeError:
        inception = r.get('entities').get(wikidata_id).get('claims').get('P577')[0].get('mainsnak').get('datavalue').get('value').get('time')[1:5]

    author_id = r.get('entities').get(wikidata_id).get('claims').get('P50')[0].get('mainsnak').get('datavalue').get('value').get('id')
    author_r = r = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{author_id}.json').json()
    author = author_r.get('entities').get(author_id).get('labels').get('pl').get('value')
    
    temp_dict = {'autor': author,
                 'tytuł': title,
                 'data publikacji oryginału': inception}
    
    results_list.append(temp_dict)
























