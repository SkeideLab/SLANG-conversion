from pathlib import Path
from shutil import rmtree
from zipfile import ZipFile

import pandas as pd
from pydicom import dcmread

bids_dir = Path(__file__).parent.parent.parent
sourcedata_dir = bids_dir / 'sourcedata'
zip_files = sorted(list(sourcedata_dir.glob('*/*_mri.zip')))

sub_dirs = sorted(list(bids_dir.glob('sub-*')))
participant_ids = [sub_dir.name.strip('sub-') for sub_dir in sub_dirs]

dfs = []

for participant_id in participant_ids:

    pattern = f'*/{participant_id}_*_mri.zip'
    zip_files = sorted(list(sourcedata_dir.glob(pattern)))
    zip_file = zip_files[0]

    basename = zip_file.stem

    participant = basename.split('_')[0]

    if basename.startswith('SA'):
        group = 'Reading'
    elif basename.startswith('SO'):
        group = 'Math'
    else:
        raise ValueError(f'Unknown participant group for {zip_file}')

    tmp_dir = sourcedata_dir.parent.parent / f'tmp_{basename}'
    tmp_dir.mkdir()

    with ZipFile(zip_file, 'r') as zip:
        zip.extractall(tmp_dir)

    dcm_files = list(tmp_dir.glob('**/*.dcm'))
    assert len(dcm_files) > 0, f'No dcm files found in {zip_file}'

    dcm = dcmread(dcm_files[0])

    age = int(dcm.PatientAge.strip('Y'))
    sex = dcm.PatientSex

    df = pd.DataFrame({'participant_id': [participant],
                       'group': [group],
                       'age': [age],
                       'sex': [sex]})
    dfs.append(df)

    rmtree(tmp_dir)

df = pd.concat(dfs, ignore_index=True)
df_file = bids_dir / 'participants.tsv'
df.to_csv(df_file, sep='\t', index=False)
