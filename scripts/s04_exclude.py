from argparse import ArgumentParser
from pathlib import Path

import pandas as pd
from datalad.api import Dataset
import re


def parse_args():
    """Parses arguments from the command line."""

    parser = ArgumentParser()
    parser.add_argument('-d', '--bids_dir', required=True,
                        help='Directory of the BIDS dataset')
    parser.add_argument('-p', '--fd_perc', default=10., required=False,
                        help='Threshold of volumes percentage > fd to exclude')
    parser.add_argument('-z', '--move_zip', default='True', required=False,
                        help='Move zip files to exclude (if false move bids files)')
    args = parser.parse_args()
    return args


def main():

    # Parse command line arguments
    args = parse_args()
    bids_dir = Path(args.bids_dir)
    bids_ds = Dataset(bids_dir)
    fd_perc = float(args.fd_perc)
    move_zip_exclude = args.move_zip

    bad_files = identify_scans_to_remove(bids_dir, fd_perc)

    if bad_files:

        print(f'Removing high movement scans (fd_perc > {fd_perc}):')
        # two options:
        # 1. delete bids files and move zip to exclude
        # 2. move bids files to exclude
        if move_zip_exclude.casefold() == 'true':
            print('\n'.join([str(f) for f in bad_files]))
            bids_ds.remove(
                bad_files, message='Remove high movement scans from BIDS ', reckless='undead')
            changed_files = move_zip_to_exclude(
                bids_ds, bad_files)
            message = 'Moving zips to sourcedata/exclude'
        else:
            changed_files = move_scans_to_exclude(bids_ds,  bad_files)
            message = 'Moving high motion files to sourcedata'
        bids_ds.save(path=changed_files,
                     message=message)
    else:

        print('No high movement scans to remove.')


def identify_scans_to_remove(bids_dir, fd_perc):
    """Identify paths of the scans

    Parameters
    ----------
    bids_dir : Path object
        path to bids root directory
    fd_perc : float
        Threshold of volumes percentage > fd to exclude

    Returns
    -------
    list
        paths to bad scans
    """
    # Read group level QC report
    qc_file = bids_dir / 'derivatives/mriqc/group_bold.tsv'
    qc = pd.read_csv(qc_file, delimiter='\t')

    # Get problematic participants and sessions
    bad_names = qc['bids_name'][qc['fd_perc'] > fd_perc]

    files_to_remove = [bad_file
                       for bad_name
                       in bad_names
                       for bad_file
                       in bids_dir.glob('sub-*/**/' + bad_name + '*')]
    return files_to_remove


def move_scans_to_exclude(bids_ds, old_files):
    """Move files to sourcedata/exclude

    Parameters
    ----------
    bids_ds : datalad.api.Dataset
        bids root dataset
    old_files : list
        paths to scans to move

    Returns
    -------
    list
        paths of changed files
    """
    new_dir = bids_ds.pathobj / 'sourcedata' / 'exclude'
    changed_files = []
    for old_file in old_files:
        bids_ds.unlock(old_file)
        new_file = new_dir / old_file.relative_to(bids_ds.pathobj)
        new_file.parent.mkdir(parents=True, exist_ok=True)
        print(f'Moving file `{old_file}` to `./sourcedata/exclude/`')
        old_file.rename(new_file)
        changed_files += [old_file, new_file]

    return changed_files


def move_zip_to_exclude(bids_ds, bad_files):
    """Move zip files of bad scans to sourcedata/exclude.

    Parameters
    ----------
    bids_ds : datalad.api.Dataset
        bids root dataset
    bad_files : list of path objects
        filenames of bad scans

    Returns
    -------
    list
        paths of changed files
    """
    # Get participants and sessions
    bad_participants = [
        re.search(r'(?<=sub-)[0-9A-Za-z]*', str(f)).group(0)
        for f in bad_files]
    bad_sessions = [re.search(r'(?<=ses-)[0-9A-Za-z]*', str(f)).group(0)
                    for f in bad_files]
    changed_files = []
    for participant, session in set(zip(bad_participants, bad_sessions)):
        old_dir = bids_ds.pathobj / 'sourcedata' / session
        new_dir = bids_ds.pathobj / 'sourcedata' / 'exclude' / session
        new_dir.mkdir(parents=True, exist_ok=True)
        old_files = old_dir.glob(participant + '*.zip')
        for old_file in old_files:
            bids_ds.unlock(old_file)
            new_file = new_dir / old_file.name
            print(f'Moving file `{old_file}` to `{new_dir}`')
            old_file.rename(new_file)
            changed_files += [old_file, new_file]
    return changed_files


# Run
if __name__ == '__main__':
    main()
