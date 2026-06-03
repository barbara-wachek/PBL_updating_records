'''Funkcja do odrzucenia rekordów. Pozwala odrzucić rekordy, które już były przetworzone. 
Warunkiem jest data ostatniego importu. Rekordy mają powstać tego dnia lub po nim, 
albo zostać zmodyfikowane tego dnia lub po nim
'''


#%% Import


from pathlib import Path
from datetime import date
from tqdm import tqdm



#%%

run_date = date.today().strftime("%Y-%m-%d")


# folder z plikami MRK
folder_path = Path(
    rf"C:\Users\barba\Documents\GitHub\PBL_updating_records\data\{run_date}"
)

folder_path.mkdir(parents=True, exist_ok=True)

print(f"Używam folderu: {folder_path}")


# data ostatniego importu
THRESHOLD_008 = "260127"
THRESHOLD_005 = "20260127000000"

output_folder = Path(
    rf"C:\Users\barba\Documents\GitHub\PBL_updating_records\data\filtered_records_{run_date}"
)

output_folder.mkdir(parents=True, exist_ok=True)


#%%

existing_files = list(output_folder.glob("filtered_*.mrk"))

if existing_files:
    file_index = max(
        int(f.stem.split("_")[1])
        for f in existing_files
    ) + 1
else:
    file_index = 1


#%% Funkcja filtrująca

def record_is_new_or_modified(
    record,
    threshold_008=THRESHOLD_008,
    threshold_005=THRESHOLD_005
):
    creation = None
    modification = None

    for line in record.split("\n"):

        if line.startswith("=008"):
            creation = line[6:12]

        elif line.startswith("=005"):
            modification = line[6:20]

    return (
        (creation is not None and creation >= threshold_008)
        or
        (modification is not None and modification >= threshold_005)
    )


#%% Poprawne dzielenie MRK po rekordach (=LDR)

def split_mrk_records(text):
    records = []
    current = []

    for line in text.splitlines():

        if line.startswith("=LDR") and current:
            records.append("\n".join(current))
            current = []

        current.append(line)

    if current:
        records.append("\n".join(current))

    return records


#%% przetwarzanie

batch_size = 200_000
batch = []

all_records = 0
kept_records = 0


# WYKLUCZENIE OUTPUTÓW (jeśli kiedykolwiek trafiły do folderu wejściowego)
mrk_files = [
    f for f in folder_path.glob("*.mrk")
    if not f.name.startswith("filtered_records_")
]

for mrk_file in tqdm(mrk_files, desc="Pliki MRK"):

    with open(mrk_file, "r", encoding="utf-8") as f:
        content = f.read()

    records = split_mrk_records(content)
    all_records += len(records)

    for record in tqdm(records, desc=f"Rekordy {mrk_file.name}", leave=False):

        if record_is_new_or_modified(record):
            batch.append(record)
            kept_records += 1

        if len(batch) >= batch_size:

            output_file = output_folder / f"filtered_{file_index:03}.mrk"

            with open(output_file, "w", encoding="utf-8") as f:
                f.write("\n\n".join(batch))

            batch = []
            file_index += 1


# zapis reszty
if batch:

    output_file = output_folder / f"filtered_{file_index:03}.mrk"

    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n\n".join(batch))


#%% podsumowanie

print("\n--- PODSUMOWANIE ---")
print(f"Wszystkich rekordów: {all_records}")
print(f"Zachowanych: {kept_records}")
print(f"Odrzuconych: {all_records - kept_records}")
print(f"Folder wynikowy: {output_folder}")