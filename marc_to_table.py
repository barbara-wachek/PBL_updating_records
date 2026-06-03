from my_functions import mrc_to_mrk
from tqdm import tqdm
from pathlib import Path


#%% 
# folder z plikami źródłowymi .mrc, lokalny (podmienić przy kolejnym imporcie)
input_path = Path(r"C:\Users\barba\Desktop\PBL_importy\BN_2026-06-01")

# folder docelowy dla plików .mrk, repo git (podmienić przy kolejnym imporcie)
output_path = Path(r"C:\Users\barba\Documents\GitHub\PBL_updating_records\data\2026-06-01")


output_path.mkdir(parents=True, exist_ok=True)


files = list(input_path.glob("*.mrc"))

for file_path in tqdm(files):
    path_mrk = output_path / f"{file_path.stem}.mrk"
    mrc_to_mrk(file_path, path_mrk)





