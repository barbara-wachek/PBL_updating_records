#Kod do przetworzenia wszystkich rekordów z ELB (polskich) i utworzeniach z nich tabel do oglądu przez ludzi

#%% import

from pymarc import MARCReader
import pandas as pd
import glob
from tqdm import tqdm

#%% def

all_files = glob.glob('C:/Users/PBL_Basia/Documents/My scripts/PBL_updating_records/data/aktualny_ELB_2025-04/*.mrk')


file = "C:/Users/PBL_Basia/Documents/My scripts/PBL_updating_records/data/aktualny_ELB_2025-04/mrc/pl_bn_articles2_2025-03-04.mrc"


# Najpierw policz ile jest rekordów:
with open(file, 'rb') as fh:
    total_records = sum(1 for _ in MARCReader(fh))



records_list = []
with open(file, 'rb') as fh:
    reader = MARCReader(fh, to_unicode=True, force_utf8=True)
    
    for record in tqdm(reader, total=total_records, desc="Przetwarzanie rekordów"):
        row = {
            'LDR': record.leader if record.leader else '',
            '001': record.get_fields('001') if record.get_fields('001') else '',
            '100': record.get_fields('100') if record.get_fields('100') else '',
            '245': record.get_fields('245') if record.get_fields('245') else '',
            '260': record.get_fields('260') if record.get_fields('260') else '',
            '630': record.get_fields('630') if record.get_fields('630') else '',
            '650': record.get_fields('650') if record.get_fields('650') else '',
            '773': record.get_fields('773') if record.get_fields('773') else '',
            '955': record.get_fields('955') if record.get_fields('955') else '',
            '995': record.get_fields('995') if record.get_fields('995') else '',
        }
        records_list.append(row)

df = pd.DataFrame(records_list)

records_list[0:8]
