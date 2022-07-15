import json
from pathlib import Path
from sys import executable

from datalad.api import Dataset

from helpers import create_sub_ds, download_datashare, submit_job

# Read study-specific inputs from `run_params.json`
with open(f'{Path(__file__).parent.resolve()}/run_params.json', 'r') as fp:
    run_params = json.load(fp)

# Get Datalad dataset
bids_dir = Path(__file__).parent.parent.resolve()
bids_ds = Dataset(bids_dir)

# Create sub-dataset for derivatives (i.e., QC reports)
deriv_ds = create_sub_ds(bids_ds, 'derivatives')

# Create outputstore to store intermediate results from batch jobs
ria_dir = bids_dir / '.outputstore'
if not ria_dir.exists():
    ria_url = f'ria+file://{ria_dir}'
    bids_ds.create_sibling_ria(ria_url, name='output', alias='bids')
    deriv_ds.create_sibling_ria(ria_url, name='output', alias='derivatives')
    bids_ds.push(to='output')
    deriv_ds.push(to='output')

# Get paths of the datasat siblings in the outputstore
bids_remote = bids_ds.siblings(name='output')[0]['url']
deriv_remote = deriv_ds.siblings(name='output')[0]['url']

# Make sure that containers are available for the batch jobs
code_dir_name = Path(__file__).parent.name
containers_path = code_dir_name + '/containers/images/'
containers_dict = {
    'heudiconv': containers_path + 'repronim/repronim-reproin--0.9.0.sing',
    'bidsonym': containers_path + 'bids/bids-bidsonym--0.0.4.sing',
    'mriqc': containers_path + 'bids/bids-mriqc--0.16.1.sing'}
bids_ds.get(containers_dict.values())

# Define directory for log files of SLURM jobs
log_dir = bids_dir / code_dir_name / 'logs'

# Download new raw data from DataShare
participants_sessions = download_datashare(
    run_params['datashare_dir'], bids_ds)

# # Select a subset of participants/sessions for debugging
# participants_sessions = [('SA20', '01')]

# DICOM to BIDS conversion, defacing, and participant level quality control
# This happens in parallel for all participant/session pairs
script = f'{bids_dir}/code/s01_convert_deface_qc.sh'
containers = containers_dict.values()
remotes = [bids_remote, deriv_remote]
fd_thres = run_params['fd_thres']
job_name = 's01_convert_deface_qc'
job_ids = []
for participant, session in participants_sessions:
    args = [script, bids_dir, *containers, *remotes,
            participant, session, fd_thres]
    job_id = submit_job(
        args,
        job_name=f's01_convert_deface_qc_sub-{participant}_ses-{session}',
        log_dir=log_dir)
    job_ids.append(job_id)


# Merge branches back into the dataset once they've finished
script = f'{bids_dir}/code/s02_merge.sh'
job_name = 's02_merge'
job_id = submit_job(
    [script, bids_dir, *job_ids], dependency_jobs=job_ids,
    dependency_type='afterany', job_name=job_name, log_dir=log_dir)

# Group level quality control
script = f'{bids_dir}/code/s03_qc_group.sh'
container = containers_dict['mriqc']
fd_thres = run_params['fd_thres']
job_name = 's03_qc_group'
job_id = submit_job(
    [script, bids_dir, container, fd_thres], dependency_jobs=job_id,
    dependency_type='afterok', job_name=job_name, log_dir=log_dir)

# Discard high-movement scans
script = f'{bids_dir}/code/s04_exclude.py'
fd_perc = run_params['fd_perc']
job_name = 's04_exclude'
job_id = submit_job(
    [executable, script, '-d', bids_dir, '-p', fd_perc],
    dependency_jobs=job_id, dependency_type='afterok', job_name=job_name,
    log_dir=log_dir)

# Copy `events.tsv` files created by PsychoPy into the BIDS structure
if run_params['events_file_pattern'] is not None:
    script = f'{bids_dir}/code/s05_copy_events.py'
    job_name = 's05_copy_events'
    events_file_pattern = f"\"{run_params['events_file_pattern']}\""
    job_id = submit_job(
        [executable, script, '-d', bids_dir, '-p', events_file_pattern],
        dependency_jobs=job_id, dependency_type='afterok', job_name=job_name,
        log_dir=log_dir)
