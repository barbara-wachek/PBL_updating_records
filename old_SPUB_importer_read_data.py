import os.path
import sys
from my_functions import mrc_to_mrk
import io
from tqdm import tqdm
import regex as re
from collections import Counter

#def

def read_MARC21(path_mrk, encoding='UTF-8'):
    path_mrc = path_mrk.replace('.mrk', '.mrc')
    if not os.path.isfile(path_mrk):
        mrc_to_mrk(path_mrc, path_mrk)
    elif not os.path.isfile(path_mrc):
        sys.exit('No MARC21 file in the directory!') 
    marc_list = io.open(path_mrk, 'rt', encoding = encoding).read().splitlines()
    mrk_list = []
    for row in marc_list:
        if row.startswith('=LDR'):
            mrk_list.append([row])
        else:
            if row:
                mrk_list[-1].append(row)
    return mrk_list

def get_list_of_people(marc21_list, fields_tuple, regex_replace, top=0):
    list_of_people = []           
    for sublist in tqdm(marc21_list):
        for el in sublist:
            if el.startswith(fields_tuple):
                el = re.sub(regex_replace, '', el[8:]).replace('$2DBN', '').strip()
                if len(el) > 3:
                    list_of_people.append(el)
    if top>0:
        list_of_people = Counter(list_of_people).most_common(top)
        list_of_people = [e[0] for e in list_of_people]
    list_of_people = list(set(list_of_people))
    return list_of_people
    
def get_list_of_records(marc21_list, list_of_people, fields_tuple, regex_replace):
    list_of_people = list_of_people.copy()
    bibliographical_records = []
    for sublist in tqdm(marc21_list):
        for el in sublist:
            if el.startswith(fields_tuple):
                el = re.sub(regex_replace, '', el[8:]).replace('$2DBN', '').strip()
                for osoba in list_of_people:
                    if osoba in el:
                        bibliographical_records.append(sublist)
                        list_of_people.remove(osoba)
                        break
    return bibliographical_records
    



            




























