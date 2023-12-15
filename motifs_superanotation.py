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
import pandas as pd

#%%

aka_motifs = gsheet_to_df('1yefQ2CKTWFW4LcIIilxpflbn9Ic8QFe72hRfb50MMoo', 'AKa')
cr_motifs = gsheet_to_df('1yefQ2CKTWFW4LcIIilxpflbn9Ic8QFe72hRfb50MMoo', 'CR')

aka_motifs = aka_motifs.loc[aka_motifs['status'] == 'done']
aka_motifs = aka_motifs[['bookid', 'text', 'motif_gt', 'motif_WTF', 'motif_pred', 'motif_pred_correct', 'motif_pred_remaining', 'motif_pred_WTF', 'motif_sugestions_from_list']]

cr_motifs = cr_motifs.loc[cr_motifs['status'] == 'done']
cr_motifs = cr_motifs[['bookid', 'text', 'motif_gt', 'motif_WTF', 'motif_pred', 'motif_pred_correct', 'motif_pred_remaining', 'motif_pred_WTF', 'motif_sugestions_from_list']]

def right_motifs(x):
    m_gt = [e.strip() for e in x['motif_gt'].split(',')]
    try:
        m_gt_wtf = [e.strip() for e in x['motif_WTF'].split(',')]
    except AttributeError:
        m_gt_wtf = []
    m_gt = [e for e in m_gt if e not in m_gt_wtf]
    m_pred = [e.strip() for e in x['motif_pred'].split(',')]
    try:
        m_pred_wtf = [e.strip() for e in x['motif_pred_WTF'].split(',')]
    except AttributeError:
        m_pred_wtf = []
    m_pred = [e for e in m_pred if e not in m_pred_wtf]
    try:
        m_suggest = [e.strip() for e in x['motif_sugestions_from_list'].split(',')]
    except AttributeError:
        m_suggest = []
    return set(m_gt + m_pred + m_suggest)

aka_motifs['aka'] = aka_motifs.apply(lambda x: right_motifs(x), axis=1)
aka_motifs = aka_motifs[['bookid', 'text', 'aka']]
aka_motifs = aka_motifs.explode('aka').reset_index(drop=True)

cr_motifs['cr'] = cr_motifs.apply(lambda x: right_motifs(x), axis=1)
cr_motifs = cr_motifs[['bookid', 'text', 'cr']]
cr_motifs = cr_motifs.explode('cr').reset_index(drop=True)

cr_grouped = cr_motifs.groupby(['bookid', 'text'])
aka_grouped = aka_motifs.groupby(['bookid', 'text'])

final_df = pd.DataFrame()
for name, group in tqdm(cr_grouped, total=len(cr_grouped)):
    # name = ('wojdowski-chleb-rzucony-umarlym', '—  Czy tak nie było zawsze i wszędzie ? Dzieci moje, wnuki moje. Po co biadać i rwać szaty ? Biadać słowami nadużytymi ? Rwać szaty, co rozdarte ?   Każda przemoc stroiła się w orle pióra i każda niosła przed sobą rózgę sprawiedliwości.  Stare dzieje, nowe dzieje. Szaleństwo ucicha, kiedy zmęczy się własnym krzykiem. Jest przerwa na prawo, przerwa na pokój i przerwa na cywilizację. A potem, po przerwie ? Mam narzekać ? Zmęczony jestem. Na kogo i jakimi słowami ? Mam nienawidzić ? Dni moje policzone. Nienawidzi syty, głodny pochyla głowę i nie patrzy nikomu w oczy.')
    # group = cr_grouped.get_group(name)
    cr_m = set(group['cr'].to_list())
    
    try:
        aka_group = aka_grouped.get_group(name)
        aka_m = set(aka_group['aka'].to_list())
        both = cr_m & aka_m
        only_cr = cr_m - aka_m
        only_aka = aka_m - cr_m
        temp_df = []
        
        for e in both:
            temp_df.append([name[0], name[1], e, e])
        for e in only_cr:
            temp_df.append([name[0], name[1], e, None])
        for e in only_aka:
            temp_df.append([name[0], name[1], None, e])
        
        iter_df = pd.DataFrame(temp_df, columns = ['bookid', 'text', 'cr', 'aka'])
        final_df = pd.concat([final_df, iter_df])
    except KeyError:
        pass
    























