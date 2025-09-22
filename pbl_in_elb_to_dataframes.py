import pandas as pd
from pymarc import MARCReader
from pymarc import record, Field, MARCWriter
from pymarc import Record, Field, Subfield, MARCWriter, MARCReader, TextWriter
from pymarc import exceptions as exc
import os

#%%

#PLAN: 
    #1. Konwersja pliku mrk do mrc - funkcja DP
    #2. Przetwarzanie mrc
        #a) wybranie odpowiednich pól - chyba nie wszystkie
        #b) wyraźne ustalenie co jest ID, aby potem po ID wyrzucać niepotrzebne rekordy
        #c) podzielenie tabelek na takie po maks 10k rekordów 
        #d) albo wybranie tylko rekordów z okreslonych działow? gatunków? np. wybrać wszystkie reportaże do analizy 
        
        

#Poniżej wklejamy sciezke pliku ktory chcemy przekonwertowac z formatu mrk na mrc
# mrk_file_path = "C:\\Users\\PBL_Basia\\Documents\\My scripts\\PBL_updating_records\\data\\aktualny_ELB_2025-04\pl_bn_articles_2025-03-04.mrk" - cos nie tak z tym plikiem
mrk_file_path ="C:\\Users\\PBL_Basia\\Documents\\My scripts\\PBL_updating_records\\data\\aktualny_ELB_2025-04\\pl_bn_books_2024-02-08.mrk"

#Poniżej docelowa sciezka (w tym folder i nazwa) pliku wynikowego w formacie mrc
mrc_file_path = "C:\\Users\\PBL_Basia\\Documents\\My scripts\\PBL_updating_records\\data\\aktualny_ELB_2025-04\mrc\pl_bn_books_2024-02-08.mrc"


# Funkcja do konwersji mrk na mrc (format binarny, lepszy do przetwarzania) - kod Darka

def mark_to_list(path):
    path = mrk_file_path
    records = []
    with open(path, 'r', encoding = 'utf-8') as mrk:
        record = []
        for line in mrk.readlines():
            if line == '\n':
                pass
            elif line.startswith('=LDR') and record: 
                records.append(record)
                record = []
                record.append(line)
            else:
                record.append(line)
        records.append(record)   
    final_output = []  
    for record in records:      
        cleared_record=[]
        for i, field in enumerate(record):
            if not field.startswith('='):
                cleared_record[-1]=cleared_record[-1][:-1]+field
                
            else:
                cleared_record.append(field)
        final_output.append(cleared_record)
        
    return final_output


def convert_mrk_to_marc(mrk_file_path, mrc_file_path):
    # with open(mrk_file_path, 'r', encoding='utf-8') as file:
    #     records = file.read().strip().split('\n\n')
    records = mark_to_list(mrk_file_path)
    records = [''.join(sublist).strip() for sublist in records]

    with open(mrc_file_path, 'wb') as marc_file:
        writer = MARCWriter(marc_file)
        for record_data in records:
            record_lines = record_data.split('\n')
            record = Record()
            record.leader = record_lines[0][6:]

            for line in record_lines[1:]:
                tag = line[1:4]
                if tag < '010':
                    value = line[6:]
                    if tag == '008':
                        value = value.replace('\\', ' ')
                    record.add_field(Field(tag=tag, data=value))
                else:
                    indicators = line[6:8].replace('\\', ' ')
                    subfields_raw = line[8:].split('$')[1:]
                    subfields_list = []
                    for subfield in subfields_raw:
                        if not subfield:  # Skip empty subfields
                            continue
                        code = subfield[0]
                        value = subfield[1:]
                        subfields_list.append(Subfield(code=code, value=value))
                    field = Field(
                        tag=tag,
                        indicators=[indicators[0], indicators[1]],
                        subfields=subfields_list
                    )
                    record.add_field(field)

            writer.write(record)
        writer.close()
    

convert_mrk_to_marc(mrk_file_path, mrc_file_path)



#%%Poniżej pracujemy juz z plikiem mrc


records_list = []

with open(mrc_file_path, 'rb') as fh:
    reader = MARCReader(fh)
    for i, rec in enumerate(reader):
        if rec:
            record_dict = {}
            # dodajemy leader
            record_dict['LDR'] = rec.leader
    
            # dynamicznie dodajemy wszystkie tagi, które istnieją
            for field in rec.get_fields():
                tag = field.tag
                record_dict[tag] = field.format_field()
    
            records_list.append(record_dict)
            if i >= 9:  # tylko 10 rekordów do podglądu
                break
        elif isinstance(reader.current_exception, exc.FatalReaderError):
            # data file format error
            # reader will raise StopIteration
            print(reader.current_exception)
            print(reader.current_chunk)
        else:
            # fix the record data, skip or stop reading:
            print(reader.current_exception)
            print(reader.current_chunk)
            # break/continue/raise

            

df_sample = pd.DataFrame(records_list)
print(df_sample)


print(os.path.exists(mrc_file_path))





with open('test/marc.dat', 'rb') as fh:
    reader = MARCReader(fh)
    for record in reader:
        if record:
            # consume the record:
            print(record.title)
        elif isinstance(reader.current_exception, exc.FatalReaderError):
            # data file format error
            # reader will raise StopIteration
            print(reader.current_exception)
            print(reader.current_chunk)
        else:
            # fix the record data, skip or stop reading:
            print(reader.current_exception)
            print(reader.current_chunk)
            # break/continue/raise































