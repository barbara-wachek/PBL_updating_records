import pandas as pd
import io
from my_functions import f, mrc_to_mrk
import re
import numpy as np
import glob
from dask import delayed, dataframe as dd
from tqdm import tqdm
from collections import Counter
from pathlib import Path


def year(row, field):
    if row['field'] == field:
        val = row['content'][7:11]
    else:
        val = np.nan
    return val

#%% new approach
    
#Change format: mrc to mrk 

# path = "C:\\Users\\barba\\Desktop\\PBL_importy\\BN_2026-01-27"
# files = [f for f in glob.glob(path + '*.mrc', recursive=True)]
# for file_path in tqdm(files):
#     path_mrk = file_path.replace('.mrc', '.mrk')
#     mrc_to_mrk(file_path, path_mrk)


#New version BW
path = Path(r"C:\\Users\\barba\\Desktop\\PBL_importy\\BN_2026-01-27")
files = list(path.glob("*.mrc"))

for file_path in tqdm(files):
    path_mrk = file_path.with_suffix(".mrk")
    mrc_to_mrk(file_path, path_mrk)






