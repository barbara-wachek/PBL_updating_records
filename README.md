# Instrukcja pobierania i przetwarzania danych z BN do PBL

## Do pobrania:
MarcEdit: https://marcedit.reeset.net/downloads
## Kolejność prac:
#### Pobranie aktualnej paczki danych z BN – bibs-all.marc
https://data.bn.org.pl/databases
#### W MarcEdit dzielimy dużą paczkę BN i przygotowujemy mniejsze pliki
Tools (menu na pasku u góry) → MARC Processing Tools → MARCSplit → podzielić na paczki po 200k
#### Konwersja plików .mrc na .mrk - kod marc_to_table.py (tylko pierwsza funkcja)
#### Uruchamiamy kod python w następującej kolejności:
1. books - libri_project_bn_books.py
2. articles - libri_project_bn_articles.py
3. chapters - libri_project_bn_chapters.py

####Na końcu wygenerowane pliki dodać do folderu Computations/ELB/aktualizacja danych w folderze z datą dzisiejszą. 
