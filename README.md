# Instrukcja pobierania i przetwarzania danych z BN do PBL

## Do pobrania:
- MarcEdit: https://marcedit.reeset.net/downloads

## Ważne foldery:

- Relacje rozdziałów i książek BN: https://drive.google.com/drive/folders/1aATJM13muNYUB6CGhvuPMPtOzjT5XE8j
- Aktualizacja danych (do przechowywania wygenerowanych plików): https://drive.google.com/drive/folders/1r5JEo0XgSJokQaXZ-rmXl8od7JA_KYca



## Kolejność prac:

### Import do ELB:
#### Pobranie aktualnej paczki danych z Biblioteki Narodowej  – bibs-all.marc
https://data.bn.org.pl/databases
#### W MarcEdit dzielimy dużą paczkę BN i przygotowujemy mniejsze pliki:
Tools (menu na pasku u góry) → MARC Processing Tools → MARCSplit → podzielić na paczki po 200k
#### Konwersja plików .mrc na .mrk - kod marc_to_table.py (tylko pierwsza funkcja)
#### Uruchamiamy kod w następującej kolejności:
1. chapters - libri_project_bn_chapters.py
2. articles - libri_project_bn_articles.py
3. books - libri_project_bn_books.py

Pamiętaj, żeby zawsze wygenerowae pliki porównywać z poprzednimi importami (żeby odsiać duplikaty).
#### Wzbogacanie danych: pliki wygenerowane powyższymi skryptami przetworzyć za pomocą kodu BN_update.py

#### Na końcu gotowe (wzbogacone) pliki dodać do folderu Computations/ELB/aktualizacja danych w folderze z datą dzisiejszą. I przekazać informację Marcinowi. (Marcin do ELB preferuje pliki w formacie mrk).

### Import do PBL (pbl.ibl.waw.pl):

#### ciąg dalszy nastąpi





### Instrukcje szczegółowe dot. skryptów:

###### libri_project_bn_chapters.py:
1. Po wykonaniu wierszy od początku do wiersza 35., wykonać krok 1. (jest niżej, pod drugim i trzecim, wiersz 132.), aby pobrać aktualny plik z relacjami (folder: Relacje rozdziałów i książek BN).
2. Wykonać kroki 2 i 3
3. W razie potrzeby podmienić ścieżki (zmienna path).
4. Pamiętaj, żeby zawsze wygenerowane pliki porównywać z poprzednimi (folder Computations -> ELB i plik bn_chapters_marc..., aby odsiać duplikaty). Gdy z czasem będzie więcej tych plików, należy je zmergować w jeden DataFrame i porównać z właśnie wygenerowanym (jeszcze przed zapisaniem).
5. Pamiętaj o aktualnych danych w polu 995, np. bn_articles_marc['995'] = '\\\\$aPBL 2004-2023: czasopisma' - musi zgadzać się rok.
6. UWAGA: prawdopodobnie generuje nieprawidłowe pliki w formacie mrc. Przekonwertować mrk za pomocą konwertera Darka lub naprawić kod.

###### libri_project_bn_books.py:
1. Pamiętać, żeby w zmiennej **newest_relations** (wiersz 280.) podstawić ID nowego pliku (wygenerowanego po wykonaniu kodu libri_project_bn_chapters.py). Plik powinien być dostępny w folderze **Relacje rozdziałów i książek BN** (link u góry).
2. Pamiętaj o aktualnych danych w polu 995, np. bn_articles_marc['995'] = '\\\\$aPBL 2004-2023: czasopisma' - musi zgadzać się rok.


###### BN_UPDATE.py:
1. Zwrócić uwagę na komentarze na początku każdej sekcji kodu. Tam jest opisane, które zadania robi osoba ze strony PBL (Basia), a które osoba ze strony ELB (Marcin). W przyszłości ten podział może się zmienić
2. Pliki niezbędne do wykonania kodu: all_650_new_karolina.xlsx, Major_genre_wszystko.xlsx. Dostępne są tutaj: https://drive.google.com/drive/folders/1XxhJRvzdYlSR-MCy-xN8uUam2OU0p1tn?usp=drive_link
