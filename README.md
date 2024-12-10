# Instrukcja pobierania i przetwarzania danych z BN do PBL

## Do pobrania:
MarcEdit: https://marcedit.reeset.net/downloads

## Ważne foldery:
Relacje rozdziałów i książek BN: https://drive.google.com/drive/folders/1aATJM13muNYUB6CGhvuPMPtOzjT5XE8j
Aktualizacja danych (Computations -> ELB) - do przechowywania wygenerowanych plików: https://drive.google.com/drive/folders/1r5JEo0XgSJokQaXZ-rmXl8od7JA_KYca



## Kolejność prac:
#### Pobranie aktualnej paczki danych z Biblioteki Narodowej  – bibs-all.marc
https://data.bn.org.pl/databases
#### W MarcEdit dzielimy dużą paczkę BN i przygotowujemy mniejsze pliki:
Tools (menu na pasku u góry) → MARC Processing Tools → MARCSplit → podzielić na paczki po 200k
#### Konwersja plików .mrc na .mrk - kod marc_to_table.py (tylko pierwsza funkcja)
#### Uruchamiamy kod python w następującej kolejności:
1. books - libri_project_bn_books.py
2. articles - libri_project_bn_articles.py
3. chapters - libri_project_bn_chapters.py

Pamiętaj, żeby zawsze wygenerowae pliki porównywać z poprzednimi

#### Na końcu wygenerowane pliki dodać do folderu Computations/ELB/aktualizacja danych w folderze z datą dzisiejszą.


##### Instrukcje szczegółowe dot. skryptów:

libri_project_bn_chapters.py: 
1. Po wykonaniu wierszy od początku do wiersza 35., wykonać krok 1. (jest niżej, pod drugim i trzecim, wiersz 132.), aby pobrać aktualny plik z relacjami (folder: https://drive.google.com/drive/folders/1aATJM13muNYUB6CGhvuPMPtOzjT5XE8j  - Relacje rozdziałów i książek BN).
2. Wykonać kroki 2 i 3
3. W razie potrzeby podmienić ścieżki (zmienna path).
4. Pamiętaj, żeby zawsze wygenerowane pliki porównywać z poprzednimi (folder Computations -> ELB i plik bn_chapters_marc..., aby odsiać duplikaty). Gdy z czasem będzie więcej tych plików, należy je zmergować w jeden df i porównać z właśnie wygenerowanym.
