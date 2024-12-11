import pandas as pd
import io
import sys
# sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from google_drive_research_folders import PBL_folder
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread as gs
from my_functions import cSplit, df_to_mrc, mrk_to_df, gsheet_to_df, marc_parser_1_field, mrc_to_mrk, marc_parser_to_dict
import datetime
import regex as re
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from tqdm import tqdm
import glob
import time
import warnings
import numpy as np
import pandasql
import ast

warnings.simplefilter(action='ignore', category=FutureWarning)

#%% def
def uproszczenie_nazw(x):
    try:
        if x.index('$') == 0:
            return x[2:]
        elif x.index('$') == 1:
            return x[4:]
    except ValueError:
        return x
    
def bn_harvesting(start_year, end_year, path, encoding, bibliographic_kind, condition_list, filtering_method):
    years = range(start_year, end_year+1)
    # path = "C:\\Users\\Barbara Wachek\\Documents\\Python Scripts\\PBL_updating_records\\data\\2024-12-05\\"
    encoding = 'utf-8'
    files = [file for file in glob.glob(path + '*.mrk', recursive=True)]
    new_list = []
    for file_path in tqdm(files):
        # file_path = files[-1]
        marc_list = io.open(file_path, 'rt', encoding = encoding).read().splitlines()
    
        mrk_list = []
        for row in marc_list:
            if row.startswith('=LDR'):
                mrk_list.append([row])
            else:
                if row:
                    mrk_list[-1].append(row)
                                   
        for sublist in mrk_list:
            if filtering_method == 'deskryptory':
                try:
                    year_biblio = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
                    bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
                    if year_biblio in years and bibliographic_level == bibliographic_kind:
                        for el in sublist:
                            if el.startswith('=650') or el.startswith('=655'):
                                if '$a' in el:
                                    if marc_parser_to_dict(el, '\\$').get('$a') in deskryptory_08_2023:
                                        new_list.append(sublist)
                                        break
                                    else:
                                        el = re.sub('\$y.*', '', el[10:]).replace('$2DBN', '').strip()
                                        if el in condition_list:
                                            new_list.append(sublist)
                                            break
                except ValueError:
                    pass
            elif filtering_method == 'rok zgonu':
                try:
                    year_biblio = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
                    bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
                    if year_biblio in years and bibliographic_level == 'm':
                        for el in sublist:
                            if el.startswith('=100'):
                                el = el[6:]
                                el = marc_parser_dict_for_field(el, '\$')
                                el = ' '.join([v for k, v in el.items() if k in ['$a', '$c', '$d']])
                                if el in condition_list:
                                    new_list.append(sublist)
                                    break
                except ValueError:
                    pass
            elif filtering_method == 'dydaktyka':
                try:
                    year_biblio = int(''.join([ele for ele in sublist if ele.startswith('=008')])[13:17])
                    bibliographic_level = ''.join([ele for ele in sublist if ele.startswith('=LDR')])[13]
                    if year_biblio in years and bibliographic_level == 'm':
                        x650s = []
                        x655s = []
                        for el in sublist:
                            if el.startswith('=650'):
                                el = re.sub('\$y.*', '', el[10:]).replace('$2DBN', '').strip()
                                x650s.append(el)
                            elif el.startswith('=655'):
                                el = re.sub('\$y.*', '', el[10:]).replace('$2DBN', '').strip()
                                x655s.append(el)
                        if any(desc in x650s for desc in condition_list[0]) and any(desc in x655s for desc in condition_list[1]):
                            new_list.append(sublist)                    
                except ValueError:
                    pass
            elif filtering_method == 'książki rozdziałów':
                try:
                    for el in sublist:
                        if el.startswith('=001') or el.startswith('=009'):
                            el = el[6:]
                            if el in condition_list:
                                new_list.append(sublist)
                                break
                except ValueError:
                    pass
            else:
                return 'Wrong filthering method!!! Choose one from: deskryptory|rok zgonu|dydaktyka|książki rozdziałów'
    return new_list

def bn_harvested_list_to_df(bn_harvesting_list):
    final_list = []
    for lista in bn_harvesting_list:
        slownik = {}
        for el in lista:
            if el[1:4] in slownik:
                slownik[el[1:4]] += f"❦{el[6:]}"
            else:
                slownik[el[1:4]] = el[6:]
        final_list.append(slownik)
    
    df = pd.DataFrame(final_list).drop_duplicates().reset_index(drop=True)
    fields = df.columns.tolist()
    fields = [i for i in fields if 'LDR' in i or re.compile('\d{3}').findall(i)]
    df = df.loc[:, df.columns.isin(fields)]
    fields.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
    df = df.reindex(columns=fields)   
    return df

def czy_polonik(x):
    polish_language = x['008'][35:38] == 'pol'
    published_in_Poland = x['008'][15:17] == 'pl'
    try:
        x041 = 'pol' in x['041']
    except (KeyError, TypeError):
        x041 = False
    try:
        x044 = 'pol' in x['044'] or 'pl' in x['044']
    except (KeyError, TypeError):
        x044 = False
    x500 = ['500', '501', '546']
    x500 = [e for e in x500 if e in x.index]
    if any('pol' in e.lower() for e in x.index.intersection(x500) if pd.notnull(e)):
        pol_in_remarks = True
    else:
        pol_in_remarks = False
    if any('polsk' in e.lower() for e in [x['650'], x['655']] if pd.notnull(e)):
        pol_descriptor = True
    else:
        pol_descriptor = False
    if any([polish_language, published_in_Poland, x041, x044, pol_in_remarks, pol_descriptor]):
        return True
    else:
        return False
    
def gatunki_literackie(x):
    for field in x:
        if pd.notnull(field):
            for element in field.split('❦'):
                element = re.sub('\$y.*', '', element[4:]).replace('$2DBN', '')
                if any(desc == element for desc in list_655):
                    return True
    return False

def rok_zgonu(x):
    try:
        return int(re.search('(?<=\- ca |\-ca |\-ok\. |\-|po )(\d+)', x).group(0))
    except (TypeError, AttributeError):
        return None
    
def marc_parser_dict_for_field(string, subfield_code):
    subfield_list = re.findall(f'{subfield_code}.', string)
    dictionary_field = {}
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        regex = f'(^)(.*?\❦{subfield_escape}|)(.*?)(\,{{0,1}})((\❦{subfield_code})(.*)|$)'
        value = re.sub(regex, r'\3', string)
        dictionary_field[subfield] = value
    return dictionary_field

#%% date
now = datetime.datetime.now()
year = now.year
month = '{:02}'.format(now.month)
day = '{:02}'.format(now.day)

#%% google authentication & google drive
#autoryzacja do tworzenia i edycji plików
gc = gs.oauth()
#autoryzacja do penetrowania dysku
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

#%% years range
starting_year = 2013
ending_year = 2024

#%% path to the newest BN dump
path = "C:\\Users\\Barbara Wachek\\Documents\\Python Scripts\\PBL_updating_records\\data\\2024-12-05\\"

#%% deskryptory BN do wydobycia rekordów
bn_deskryptory1 = gsheet_to_df('1b_DWfaMsi_10xKR8fg-0Qpzvm91p6gx3lwjgSy0zI1Q', 'deskryptory_do_filtrowania')
bn_deskryptory1 = bn_deskryptory1[bn_deskryptory1['deskryptor do filtrowania'] == 'tak']['deskryptory'].to_list()
bn_deskryptory2 = gsheet_to_df('1b_DWfaMsi_10xKR8fg-0Qpzvm91p6gx3lwjgSy0zI1Q', 'deskryptory_czasopisma')
bn_deskryptory2 = bn_deskryptory2[bn_deskryptory2['deskryptor do filtrowania'] == 'tak']['deskryptory'].to_list()
bn_deskryptory = list(set(bn_deskryptory1 + bn_deskryptory2))

bn_deskryptory1 = list(set([e.strip() for e in bn_deskryptory]))
bn_deskryptory2 = list(set(uproszczenie_nazw(e) for e in bn_deskryptory))
bn_deskryptory1 = list(set(bn_deskryptory1 + bn_deskryptory2))
bn_deskryptory2 = list(set([re.sub('\$y.*', '', e) for e in bn_deskryptory if e]))

bn_deskryptory = list(set(bn_deskryptory1 + bn_deskryptory2))

deskryptory_08_2023 = gsheet_to_df('19EiO8RrjtcVBZOt-VncWgNvibSqYGYTU3nR0_IUmCas', 'Arkusz1')
deskryptory_08_2023 = set(deskryptory_08_2023.loc[deskryptory_08_2023['ok'] == 'True'][655].to_list())

#%% deskryptory z mapowania
file_list = drive.ListFile({'q': f"'{PBL_folder}' in parents and trashed=false"}).GetList() 
file_list = drive.ListFile({'q': "'0B0l0pB6Tt9olWlJVcDFZQ010R0E' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1tPr_Ly9Lf0ZwgRQjj_iNVt-T15FSWbId' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1xzqGIfZllmXXTh2dJABeHbRPFAM34nbw' in parents and trashed=false"}).GetList()
#[print(e['title'], e['id']) for e in file_list]
mapping_files_655 = [file['id'] for file in file_list if file['title'] == 'mapowanie BN-Oracle - 655'][0]
mapping_files_650 = [file['id'] for file in file_list if file['title'].startswith('mapowanie BN-Oracle') if file['id'] != mapping_files_655]
df_650 = pd.DataFrame()
for file in tqdm(mapping_files_650):
    sheet = gc.open_by_key(file)
    df = get_as_dataframe(sheet.worksheet('deskryptory_650'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
    df_650 = pd.concat([df_650, df])

sheet = gc.open_by_key(mapping_files_655)
df_655 = get_as_dataframe(sheet.worksheet('deskryptory_655'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)

df_650 = df_650[df_650['decyzja'].isin(['margines', 'zmapowane'])][['X650', 'decyzja', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3']].reset_index(drop=True)

dict_650 = {}
for i, row in tqdm(df_650.iterrows(), total=df_650.shape[0]):
    
    lista_dzialow = row[['dzial' in i for i in row.index]].to_list()
    lista_dzialow = [e for e in lista_dzialow if pd.notnull(e)]
    lista_hasel = row[['haslo' in i for i in row.index]].to_list()
    lista_hasel = [e for e in lista_hasel if pd.notnull(e)]
    dict_650[row['X650']] = {'decyzja': row['decyzja'], 'działy PBL': lista_dzialow, 'hasła PBL': lista_hasel}

df_655 = df_655[df_655['decyzja'].isin(['zmapowane'])][['X655', 'decyzja', 'dzial_PBL_1', 'dzial_PBL_2', 'dzial_PBL_3', 'dzial_PBL_4', 'dzial_PBL_5', 'haslo_przedmiotowe_PBL_1', 'haslo_przedmiotowe_PBL_2', 'haslo_przedmiotowe_PBL_3', 'haslo_przedmiotowe_PBL_4', 'haslo_przedmiotowe_PBL_5']].reset_index(drop=True)

dict_655 = {}
for i, row in tqdm(df_655.iterrows(), total=df_655.shape[0]):
    
    lista_dzialow = row[['dzial' in i for i in row.index]].to_list()
    lista_dzialow = [e for e in lista_dzialow if pd.notnull(e)]
    lista_hasel = row[['haslo' in i for i in row.index]].to_list()
    lista_hasel = [e for e in lista_hasel if pd.notnull(e)]
    dict_655[row['X655']] = {'decyzja': row['decyzja'], 'działy PBL': lista_dzialow, 'hasła PBL': lista_hasel}
    
#dydaktyka
dydaktyka = get_as_dataframe(sheet.worksheet('dydaktyka'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1)
dydaktyka_650 = [e.split('❦') for e in dydaktyka[650] if pd.notnull(e)]
dydaktyka_650 = list(set([re.sub('\$y.*', '', e[4:]).replace('$2DBN', '') for sub in dydaktyka_650 for e in sub]))
dydaktyka_655 = [e.split('❦') for e in dydaktyka[655] if pd.notnull(e)]
dydaktyka_655 = list(set([re.sub('\$y.*', '', e[4:]).replace('$2DBN', '') for sub in dydaktyka_655 for e in sub]))

#%% BN relacje
##UWAGA! Podmienić ID pliku na aktualny plik z relacjami (wygenerowany na etapie kodu chapters) w zmiennej newest_relations. Pliku szukać w tym folderze: https://drive.google.com/drive/folders/1aATJM13muNYUB6CGhvuPMPtOzjT5XE8j 

bn_relations = gsheet_to_df('1WPhir3CwlYre7pw4e76rEnJq5DPvVZs3_828c_Mqh9c', 'relacje_rev_book').rename(columns={'id':'001'})
bn_relations = bn_relations[bn_relations['typ'] == 'book'].rename(columns={'id':'001'})

newest_relations = '1nknxNCQZurZLbNBOc8yO20H2bWt4RaV4X4wWX92RhOE'

bn_books_chapters = gsheet_to_df(newest_relations, 'relations')
bn_books_chapters = bn_books_chapters[bn_books_chapters['type'] == 'book'].rename(columns={'id':'001'})

books_id = tuple(bn_books_chapters['001'].to_list())

bn_relations = pd.concat([bn_relations, bn_books_chapters]).rename(columns={856:'856'})
bn_relations['856'] = bn_relations.groupby('001')['856'].transform(lambda x: '❦'.join(x.drop_duplicates().astype(str)))
bn_relations = bn_relations.drop_duplicates().reset_index(drop=True)

#%% harvesting BN files
            
bn_harvesting_list = bn_harvesting(starting_year, ending_year, path, 'utf8', 'm', bn_deskryptory, filtering_method='deskryptory')
bn_harvested_df = bn_harvested_list_to_df(bn_harvesting_list)

#odsianie rekordów, które nie mają nic wspólnego z PL  
bn_harvested_df['czy polonik'] = bn_harvested_df.apply(lambda x: czy_polonik(x), axis=1)
bn_harvested_df = bn_harvested_df[bn_harvested_df['czy polonik'] == True]

#odsianie druków ulotnych
druki_ulotne = bn_harvested_df[(bn_harvested_df['380'].str.contains('Druki ulotne')) & (bn_harvested_df['380'].notnull())]['001'].to_list()
bn_harvested_df = bn_harvested_df[~bn_harvested_df['001'].isin(druki_ulotne)]

#gatunki literackie
deskryptory_spoza_centrum = gsheet_to_df('1JQy98r4K7yTZixACxujH2kWY3D39rG1kFlIppNlZnzQ', 'deskryptory_spoza_centrum')['deskryptor'].to_list()
deskryptory_spoza_centrum = list(set([re.sub('\$y.*', '', e) for e in deskryptory_spoza_centrum if pd.notnull(e)]))
dodatek_do_centrum = gsheet_to_df('1JQy98r4K7yTZixACxujH2kWY3D39rG1kFlIppNlZnzQ', 'dodatek_do_centrum')
dodatek_do_centrum = dodatek_do_centrum[dodatek_do_centrum['decyzja'] == 'PRAWDA']['deskryptor'].to_list()
dodatek_do_centrum = list(set([re.sub('\$y.*', '', e) for e in dodatek_do_centrum]))

list_655 = list(set([re.sub('\$y.*', '', e) for e in dict_655.keys()]))
list_655 = [e for e in list_655 if e not in deskryptory_spoza_centrum]
list_655.extend(dodatek_do_centrum)
list_655 = list(set(list_655))

bn_harvested_df['gatunki literackie'] = bn_harvested_df[['650', '655']].apply(lambda x: gatunki_literackie(x), axis=1)

bn_harvested_df = bn_harvested_df[(bn_harvested_df['380'].str.lower().str.contains('książ|book', regex=True)) | 
                                  (bn_harvested_df['380'].isnull()) | 
                                  (bn_harvested_df['gatunki literackie'] == True)]

#%% dobre gatunki literackie
#jeśli coś ma gatunek literacki, to jest z automatu dobre
dobre1_df = bn_harvested_df[(bn_harvested_df['gatunki literackie'] == True) |
                            ((bn_harvested_df['650'].str.lower().str.contains('filolog|literatur|literac|pisar|poezj')) & bn_harvested_df['650'].notnull()) |
                            ((bn_harvested_df['655'].str.lower().str.contains('filolog|literatur|literac|pisar|poezj')) & bn_harvested_df['655'].notnull())]

#pozostałe rekordy przetwarzamy
df = bn_harvested_df[~bn_harvested_df['001'].isin(dobre1_df['001'])]

#%% frekwencje
deskryptory_z_df = df[['001', '655', '650']].reset_index(drop=True)

deskryptory_dict = {}
for i, row in tqdm(deskryptory_z_df.iterrows(), total=deskryptory_z_df.shape[0]):
    lista_deskryptorow = [e for e in f"{row['650']}❦{row['655']}".split('❦') if e != 'nan']
    deskryptory_dict[row['001']] = {'deskryptory w rekordzie': lista_deskryptorow, 'liczba deskryptorów': len(lista_deskryptorow), 'liczba dobrych deskryptorów': 0}
    
    for el in deskryptory_dict[row['001']]['deskryptory w rekordzie']:
        el = re.sub('\$y.*', '', el[4:]).replace('$2DBN', '')
        if any(desc == el for desc in bn_deskryptory):
            deskryptory_dict[row['001']]['liczba dobrych deskryptorów'] += 1
            
    deskryptory_dict[row['001']]['procent dobrych deskryptorów'] = deskryptory_dict[row['001']]['liczba dobrych deskryptorów']/deskryptory_dict[row['001']]['liczba deskryptorów']

dobre2_lista = [k for k, v in deskryptory_dict.items() if deskryptory_dict[k]['procent dobrych deskryptorów'] > 0.5]
dobre2_df = df[df['001'].isin(dobre2_lista)]

df = df[~df['001'].isin(dobre2_lista)]

#%% puste 655 z dobrymi 650

dobre3_df = df[df['655'].isnull()]
df = df[~df['001'].isin(dobre3_df['001'])]

#%% filtrowanie BN po roku zgonu

file_list = drive.ListFile({'q': f"'{PBL_folder}' in parents and trashed=false"}).GetList() 
file_list = drive.ListFile({'q': "'0B0l0pB6Tt9olWlJVcDFZQ010R0E' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1tPr_Ly9Lf0ZwgRQjj_iNVt-T15FSWbId' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1OwlXSNuKdrnB9qZDvM-UMh5ul1n5SeyL' in parents and trashed=false"}).GetList()
file_list = drive.ListFile({'q': "'1BT4mZ40m_M1NyYYUiMFOSMtwA8krkrYT' in parents and trashed=false"}).GetList()
#[print(e['title'], e['id']) for e in file_list]
mapowanie_osob = [file['id'] for file in file_list if file['title'].startswith('mapowanie_osob_bn_pbl')]

mapowanie_osob_df = pd.DataFrame()
for file in tqdm(mapowanie_osob):
    sheet = gc.open_by_key(file)
    df_osoby = get_as_dataframe(sheet.worksheet('pbl_bn'), evaluate_formulas=True).dropna(how='all').dropna(how='all', axis=1).drop_duplicates()
    df_osoby = df_osoby[df_osoby['czy_ten_sam'] != 'nie'][['pbl_id', 'BN_id', 'BN_name']]
    df_osoby['BN_name'] = df_osoby['BN_name'].str.replace('\|\(', ' (').str.replace('\;\|', '; ').str.replace('\|$', '')
    df_osoby['index'] = df_osoby.index + 1
    df_osoby = cSplit(df_osoby, 'index', 'BN_name', '\|').drop(columns='index')
    mapowanie_osob_df = pd.concat([mapowanie_osob_df, df_osoby])

mapowanie_osob_df = mapowanie_osob_df.drop_duplicates().reset_index(drop=True)

mapowanie_osob_df['rok zgonu'] = mapowanie_osob_df['BN_name'].apply(lambda x: rok_zgonu(x))
mapowanie_osob_lista = mapowanie_osob_df[(mapowanie_osob_df['rok zgonu'].notnull()) & (mapowanie_osob_df['rok zgonu'] <= 1800)]['BN_name'].drop_duplicates().to_list()

#ujednolicenie nazw z BN 100 z tabelą wzorcową

bn_harvesting_list = bn_harvesting(starting_year, ending_year, path, 'utf8', 'm', mapowanie_osob_lista, filtering_method='rok zgonu')
dobre4_df = bn_harvested_list_to_df(bn_harvesting_list)

dobre4_df['czy polonik'] = dobre4_df.apply(lambda x: czy_polonik(x), axis=1)
dobre4_df = dobre4_df[dobre4_df['czy polonik'] == True]
dobre4_df = dobre4_df[(dobre4_df['380'].str.lower().str.contains('książ|book', regex=True)) | 
                      (dobre4_df['380'].isnull())]

#%% dydaktyka

bn_harvesting_list = bn_harvesting(starting_year, ending_year, path, 'utf8', 'm', (dydaktyka_650, dydaktyka_655), filtering_method='dydaktyka')
dobre5_df = bn_harvested_list_to_df(bn_harvesting_list)

#%% wydobycie książek rozdziałów - mieli ponad godzinę

bn_harvesting_list = bn_harvesting(starting_year, ending_year, path, 'utf8', 'm', books_id, filtering_method='książki rozdziałów')
dobre6_df = bn_harvested_list_to_df(bn_harvesting_list)

#%% połączenie zbiorów

bn_harvested = pd.concat([dobre1_df, dobre2_df, dobre3_df, dobre4_df, dobre5_df]).drop_duplicates().reset_index(drop=False)
dobre6_df = dobre6_df[~dobre6_df['001'].isin(bn_harvested['001'])]
bn_harvested = pd.concat([bn_harvested, dobre6_df])

#%% odsiewanie po deskryptorach
ids = bn_harvested[(bn_harvested['650'].str.contains('Opera (przedstawienie)', regex=False)) & (bn_harvested['655'].str.contains('Program teatralny'))]['001'].to_list()
ids += bn_harvested[(bn_harvested['650'].str.contains('Turystyka dziecięca', regex=False)) & (bn_harvested['655'].str.count('\\$a') == 1) & (bn_harvested['655'].str.count('\\$y') == 0)]['001'].to_list()
ids += bn_harvested[bn_harvested['LDR'].str[6] == 'g']['001'].to_list()
ids += bn_harvested[(bn_harvested['245'].str.contains('$a[Scena ze spektaklu', regex=False)) & ((bn_harvested['650'].str.contains('Fotografia teatralna', regex=False)) | 
          (bn_harvested['655'].str.contains('Fotografia teatralna', regex=False)))]['001'].to_list()
ids += bn_harvested[(bn_harvested['655'].str.contains('Słownik frazeologiczny', regex=False)) & (~bn_harvested['655'].str.contains('języka polskiego', regex=False, na=False))]['001'].to_list()
#Zacząć od tego
ids += bn_harvested[((bn_harvested['655'].str.contains('Rozważania i rozmyślania religijne', regex=False)) & (bn_harvested['655'].str.count('\\$a') == 1)) |((bn_harvested['655'].str.contains('Rozważania i rozmyślania religijne', regex=False)) & (bn_harvested['655'].str.contains('Modlitwa|Modlitewnik')) & (bn_harvested['655'].str.count('\\$a') == 2))]['001'].to_list()
ids += bn_harvested[(bn_harvested['650'].str.contains('Pedagogika$x', regex=False)) & (bn_harvested['655'].isnull())]['001'].to_list()
ids += bn_harvested[(bn_harvested['650'].str.contains('Drama (pedagogika)', regex=False)) & (bn_harvested['655'].isnull())]['001'].to_list()

ids = list(set(ids))  

bn_harvested = bn_harvested[~bn_harvested['001'].isin(ids)]

podejrzane_deskryptory = ['Muzyka (przedmiot szkolny)', 'Edukacja artystyczna', 'Odbitka barwna', 'Korespondencja handlowa', 'Gatunek zagrożony', 'Reportaż radiowy', 'Art brut', 'Budownictwo$xprojekty$xprzekłady', 'Język środowiskowy$xprzekłady']

ids = []
for el in podejrzane_deskryptory:
    test = bn_harvested[(bn_harvested['650'].str.contains(el, regex=False)) | 
              (bn_harvested['655'].str.contains(el, regex=False))]['001'].to_list()
    ids += test
ids = list(set(ids))    

bn_harvested = bn_harvested[~bn_harvested['001'].isin(ids)]

# bn_harvested.to_excel(f'data/bn_harvested_{year}_{month}_{day}.xlsx', index=False)

# df_to_mrc(bn_harvested.drop(columns=['index', 'czy polonik', 'gatunki literackie']), '❦', f'bn_harvested_{year}_{month}_{day}.mrc', f'bn_harvested_errors_{year}_{month}_{day}.txt')





#%%

bn_cz_mapping = pd.read_excel(r"C:\Users\Barbara Wachek\Documents\Python Scripts\PBL_updating_records\data\bn_cz_mapping.xlsx")
fields_to_remove = bn_cz_mapping[bn_cz_mapping['cz'] == 'del']['bn'].to_list()
fields_to_remove = [e if e[0] != 'X' else e[1:] for e in fields_to_remove]

bn_books_marc_total = bn_harvested.drop(['852', '856'], axis = 1)
bn_books_marc_total = bn_books_marc_total.loc[:, ~bn_books_marc_total.columns.isin(fields_to_remove)]
bn_books_marc_total = pd.merge(bn_books_marc_total, bn_relations, on='001', how='left')

field_list = bn_books_marc_total.columns.tolist()
field_list = [i for i in field_list if any(a == i for a in ['LDR', 'AVA']) or re.compile('\d{3}').findall(i)]
bn_books_marc_total = bn_books_marc_total.loc[:, bn_books_marc_total.columns.isin(field_list)]
field_list.sort(key = lambda x: ([str,int].index(type("a" if re.findall(r'\w+', x)[0].isalpha() else 1)), x))
bn_books_marc_total = bn_books_marc_total.reindex(columns=field_list)   
bn_books_marc_total = bn_books_marc_total.reset_index(drop=True)

bn_books_marc_total['008'] = bn_books_marc_total['008'].str.replace('\\', ' ')
if bn_books_marc_total['009'].dtype == np.float64:
        bn_books_marc_total['009'] = bn_books_marc_total['009'].astype(np.int64)
        
bn_books_marc_total['995'] = '\\\\$aPBL 2013-2023: książki'

#Porównanie z ostatnio wygenerowym plikiem bn_books_marc z 2021 roku (aby odsiać duplikaty) folder ELB w Computations
bn_books_marc_old = pd.read_excel(r"C:\Users\Barbara Wachek\Documents\Python Scripts\PBL_updating_records\data\old_imports\bn_books_marc_2021-07-01.xlsx", sheet_name='Sheet1')
#Stworzenie list z ID z obu df: starego i nowego:
list_bn_books_marc_old_ID = set(bn_books_marc_old['001'].dropna().tolist())
list_bn_books_marc_total_ID = set(bn_books_marc_total['001'].dropna().tolist())

list_new_records_only_ID = list(list_bn_books_marc_total_ID - list_bn_books_marc_old_ID)

#Filtrowanie bn_books_marc_total, aby uwzględnić tylko rekordy nowe (których nie zaimportowano we wcześniejszym imporcie)

bn_books_marc_final = bn_books_marc_total[bn_books_marc_total['001'].isin(list_new_records_only_ID)]
bn_books_marc_final.drop_duplicates

# len(set(bn_books_marc_final['001']))

#%% Zapisanie do xlsx i mrc i przekształcenie mrc na mrk

bn_books_marc_final.to_excel('data/bn_books_marc.xlsx', index=False)

df_to_mrc(bn_books_marc_final, '❦', f'data/libri_marc_bn_books_{year}-{month}-{day}.mrc', f'data/libri_bn_books_errors_{year}-{month}-{day}.txt')
mrc_to_mrk(f'data/libri_marc_bn_books_{year}-{month}-{day}.mrc', f'data/libri_marc_bn_books_{year}-{month}-{day}.mrk')


#%%errors handling

with open(f'data/libri_bn_books_errors_{year}-{month}-{day}.txt', encoding='utf8') as file:
    errors = file.readlines()
    
errors = [ast.literal_eval(re.findall('\{.+\}', e)[0]) for e in errors if e != '\n']
errors = [{(k):(v if k not in ['856', 856] else re.sub('(\:|\=)(❦)', r'\1 ',v)) for k,v in e.items()} for e in errors]
if errors:
    df2 = pd.DataFrame(errors)
    df2['008'] = df2['008'].str.replace('\\', ' ')
    if df2['009'].dtype == np.float64:
            df2['009'] = df2['009'].astype(np.int64)
    df_to_mrc(df2, '❦', f'data/libri_marc_bn_books2_{year}-{month}-{day}.mrc', f'data/libri_marc_bn_books2_errors_{year}-{month}-{day}.txt')
    mrc_to_mrk(f'data/libri_marc_bn_books2_{year}-{month}-{day}.mrc', f'data/libri_marc_bn_books2_{year}-{month}-{day}.mrk')


































