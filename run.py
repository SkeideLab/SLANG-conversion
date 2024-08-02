# %%
# Load modules
import json
from pathlib import Path
from sys import executable

from datalad.api import Dataset

from scripts.helpers import create_sub_ds, download_datashare, submit_job

# %%
# Get file paths and parameters
code_dir = Path(__file__).parent.resolve()
log_dir = code_dir / 'logs'

bids_dir = code_dir.parent
bids_ds = Dataset(bids_dir)

deriv_ds = create_sub_ds(bids_ds, sub_ds_name='derivatives')

with open(code_dir / 'run_params.json', 'r') as params_file:
    run_params = json.load(params_file)

# %%
# Create outputstore to store intermediate results from batch jobs
ria_dir = bids_dir / '.outputstore'
if not ria_dir.exists():
    ria_url = f'ria+file://{ria_dir}'
    bids_ds.create_sibling_ria(ria_url, name='output', alias='bids',
                               new_store_ok=True)
    deriv_ds.create_sibling_ria(ria_url, name='output', alias='derivatives',
                                new_store_ok=True)
    bids_ds.push(to='output')
    deriv_ds.push(to='output')

# Get paths of the dataset siblings in the outputstore
bids_remote = bids_ds.siblings(name='output')[0]['url']
deriv_remote = deriv_ds.siblings(name='output')[0]['url']

# %%
# Make sure that containers are available for the batch jobs
containers_prefix = f'{code_dir.name}/containers/images'
containers_dict = {
    'heudiconv': f'{containers_prefix}/nipy/nipy-heudiconv--1.1.6.sing',
    'bidsonym': f'{containers_prefix}/bids/bids-bidsonym--0.0.4.sing',
    'mriqc': f'{containers_prefix}/bids/bids-mriqc--0.16.1.sing'}
bids_ds.get(containers_dict.values())

# %%
# Download new raw data from DataShare
datashare_dir = run_params['datashare_dir']
participants_sessions = download_datashare(datashare_dir, bids_ds)

# # Find successfully converted BIDS sessions
# participant_session_dirs = list(bids_dir.glob('sub-*/ses-*/'))
# participants_sessions_existing = [
#     (d.parent.name.replace('sub-', ''), d.name.replace('ses-', ''))
#     for d in participant_session_dirs]

# # Get all sessions where downloaded data is available
# # this will be files downloaded in this or earlier executions
# available_files = list(bids_dir.glob(f"sourcedata/*/*[0-9].zip"))
# available_sessions = [(str(s).split('/')[-1].split('_')
#                        [0], str(s).split('/')[-2]) for s in available_files]
# # Remove existing bids sessions from to-do list
# participants_sessions = list(
#     sorted(set(available_sessions).difference(participants_sessions_existing)))

# # Select a subset of participants/sessions for debugging
# participants_sessions = [('SA27', '01'), ('SA27', '02')]

# DICOM to BIDS conversion, defacing, and participant level quality control
# This happens in parallel for all participant/session pairs
script = f'{bids_dir}/code/s01_convert_deface_qc.sh'
fd_thres = run_params['fd_thres']
job_ids = []
for participant, session in participants_sessions:
    args = [script, bids_dir, bids_remote, deriv_remote,
            participant, session, fd_thres]
    job_id = submit_job(
        args,
        job_name=f's01_convert_deface_qc_sub-{participant}_ses-{session}',
        log_dir=log_dir)
    job_ids.append(job_id)

# Merge branches back into the dataset once they've finished
script = f'{bids_dir}/code/s02_merge.sh'
args = [script, bids_dir, *job_ids]
job_id = submit_job(args, dependency_jobs=job_ids, dependency_type='afterany',
                    job_name='s02_merge', log_dir=log_dir)

# Group level quality control
script = f'{bids_dir}/code/s03_qc_group.sh'
fd_thres = run_params['fd_thres']
args = [script, bids_dir, fd_thres]
job_id = submit_job(args, dependency_jobs=job_id, dependency_type='afterok',
                    job_name='s03_qc_group', log_dir=log_dir)

# Discard high-movement scans
script = f'{bids_dir}/code/s04_exclude.py'
fd_perc = run_params['fd_perc']
move_zip = run_params['move_zip'] if 'move_zip' in run_params else True
args = [executable, script, '-d', bids_dir, '-p', fd_perc, '-z', move_zip]
job_id = submit_job(args, dependency_jobs=job_id, dependency_type='afterok',
                    job_name='s04_exclude', log_dir=log_dir)

# Copy `events.tsv` files created by PsychoPy into the BIDS structure
if run_params['events_file_pattern'] is not None:
    script = f'{bids_dir}/code/s05_copy_events.py'
    events_file_pattern = "'" + run_params['events_file_pattern'] + "'"
    args = [executable, script, '-d', bids_dir, '-p', events_file_pattern]
    job_id = submit_job(
        args, dependency_jobs=job_id, dependency_type='afterok',
        job_name='s05_copy_events', log_dir=log_dir)
