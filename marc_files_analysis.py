#%% Notatki. Do analizowania pliku mrk


from pymarc import MARCReader
import pandas as pd



with open('data/libri_marc_bn_articles_2024-12-09.mrk', 'r', encoding='utf-8') as file:
    records = file.read().split("\n\n")  # Rekordy są oddzielone pustymi liniami


#Pole 773

total_records = len(records)
non_articles = []
for record in records:
    lines = record.splitlines()
    field_773 = next((line for line in lines if line.startswith('=773')), None)
    
    # Sprawdzenie, czy pole 773 istnieje
    if not field_773:
        title_line = next((line for line in lines if line.startswith('=245')), "Brak tytułu")
        non_articles.append((title_line, "Brak pola 773"))

# Wyniki
print(f"Liczba wszystkich rekordów: {total_records}")
print(f"Liczba nieartykułów: {len(non_articles)}")
for non_article in non_articles[:10]:  # Wyświetlenie pierwszych 10 nieartykułów
    print(non_article)
    
    
    
#Przekształcenie pliku mrk na listę słownikóW i DF

file_path = "data/libri_marc_bn_articles_2024-12-09.mrc"

records_list = []

# Otwieranie pliku i przetwarzanie rekordów
with open(file_path, 'rb') as file:
    reader = MARCReader(file)
    
    for record in reader:
        record_dict = {
            "leader": record.leader,
            "fields": {field.tag: field.value() for field in record.get_fields()}
        }
        records_list.append(record_dict)

# Wyświetlenie liczby rekordów
print(f"Liczba rekordów: {len(records_list)}")

# Przykładowe rekordy
for i, rec in enumerate(records_list[:3], 1):
    print(f"\nRekord {i}:\n", rec)


    
    
    
    
    
    
