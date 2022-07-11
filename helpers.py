import getpass
from pathlib import Path

import keyring
import owncloud
from datalad.api import Dataset
from simple_slurm import Slurm


def download_datashare(datashare_dir, bids_ds):
    """Downloads new zip files with raw data from MPCDF DataShare.

    Parameters
    ----------
    datashare_dir : str
        Path of the raw data starting from the DataShare root, like this:
        https://datashare.mpcdf.mpg.de/apps/files/?dir=<datashare_dir>. Data
        must be organized like <datashare_dir>/<session>/<participant>_*.zip.
    bids_ds : datalad.api.Dataset
        The BIDS dataset. New zip files will be downloaded into a 'sourcedata'
        subdataset, with separate subdirectories for each session like on
        DataShare.

    Returns
    -------
    new_participants_sessions : list of tuple
        A list with of tuples, each containing a single participant label (str)
        and session label (str) for which a new zip file was downloaded. E.g.,
        for new data from two new participants ('01' and '02'), each with one
        new session ('05'): [('01', '05'), ('02', '05')].
    """

    # Create subdataset if it doesn't exist
    source_dir = Path(bids_ds.path) / 'sourcedata'
    source_ds = Dataset(source_dir)
    if not source_ds.is_installed():
        bids_ds.create('sourcedata', cfg_proc='text2git')

    # Get DataShare login credentials
    datashare_user = getpass.getuser()
    datashare_pass = keyring.get_password('datashare', datashare_user)
    if datashare_pass is None:
        datashare_pass = getpass.getpass()
        keyring.set_password('datashare', datashare_user, datashare_pass)

    # Login to DataShare
    domain = 'https://datashare.mpcdf.mpg.de'
    datashare = owncloud.Client(domain)
    datashare.login(datashare_user, datashare_pass)

    # Create empty list / dict to track new data
    new_raw_files = []
    new_participants_sessions = set()

    # Loop over session folders on DataShare
    datashare_sessions = datashare.list(datashare_dir)
    for datashare_session in datashare_sessions:

        # Loop over files for the current session
        session = datashare_session.name
        session_dir = source_dir / session
        exclude_dir = source_dir / 'exclude' / session
        files = datashare.list(datashare_session.path)
        # -------------------------------------------------------------------
        # For testing purposes only! Comment out to process the whole dataset
        # files = files[0:2]
        # -------------------------------------------------------------------
        for file in files:

            # Explicity exclude certain file names
            if file.name.startswith('_'):
                continue

            # Download if it doesn't exist
            local_file = session_dir / file.name
            exclude_file = exclude_dir / file.name
            if not local_file.exists() and not exclude_file.exists():

                # Download zip file
                print(f'Downloading {file.path} to {session_dir}')
                session_dir.mkdir(parents=True, exist_ok=True)
                datashare.get_file(file, local_file)

                # Keep track of new data
                new_raw_files.append(local_file)
                participant = file.name.split('_')[0]
                new_participants_sessions.add((participant, session))

    # Save new zip files
    source_ds.save(new_raw_files, message='Add raw data from DataShare')
    bids_ds.save('sourcedata', message='Add raw data from DataShare')

    # Return new data as (participant, session) tuples
    new_participants_sessions = sorted(list(new_participants_sessions))

    return new_participants_sessions


def submit_job(args_list, cpus=8, mem=32000, time='24:00:00', log_dir='logs/',
               dependency_jobs=[], dependency_type='afterok', job_name='job'):
    """Submits a single batch job via SLURM, which can depend on other jobs.

    Parameters
    ----------
    args_list : list
        A list of shell commands and arguments. The first element will usually
        be the path of a shell script and the following elements the input
        arguments to this script.
    cpus : int, default=8
        The number of CPUs that the batch job should use.
    mem : int, default=320000
        The amount of memory (in MB) that the abtch job should use.
    time : str, default='24:00:00'
        The maximum run time (in format 'HH:MM:SS') that the batch job can use.
        Must not exceed 24 hours.
    log_dir : str or Path, default='logs/'
        Directory to which the standard error and output messages of the batch
        job should be written.
    dependency_jobs : int or list, default=[]
        Other SLURM batch job IDs on which the current job depends. Can be used
        to create a pipeline of jobs that are executed after one another.
    dependency_type : str, default='afterok
        How to handle the 'dependency_jobs'. Must be one of ['after',
        'afterany', 'afternotok', 'afterok', 'singleton']. See [1] for further
        information. 
    job_name : str, default='job'
        Name of the slurm job that will submitted. 

    Returns
    -------
    job_id : int
        The job ID of the submitted SLURM batch job.

    Notes
    -----
    [1] https://hpc.nih.gov/docs/job_dependencies.html
    """

    # Join arguments to a single bash command
    cmd = ' '.join(str(arg) for arg in args_list)

    # Create directory for output logs
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    error = f'{log_dir}/slurm-%j-{job_name}.out'
    output = f'{log_dir}/slurm-%j-{job_name}.out'

    # Prepare job scheduler
    slurm = Slurm(cpus_per_task=cpus, error=error, mem=mem, nodes=1, ntasks=1,
                  output=output, time=time, job_name=job_name)

    # Make the current job depend on previous jobs
    if dependency_jobs != []:
        if isinstance(dependency_jobs, int):
            dependency_jobs = [dependency_jobs]
        dependency_str = ':'.join([str(job_id) for job_id in dependency_jobs])
        dependency = {dependency_type: dependency_str}
        slurm.set_dependency(dependency)

    # Submit
    print('Submitting', cmd)
    job_id = slurm.sbatch(cmd)

    return job_id
