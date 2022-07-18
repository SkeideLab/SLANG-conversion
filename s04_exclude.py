from argparse import ArgumentParser
from pathlib import Path

import pandas as pd
from datalad.api import Dataset


def parse_args():
    """Parses arguments from the command line."""

    parser = ArgumentParser()
    parser.add_argument('-d', '--bids_dir', required=True,
                        help='Directory of the BIDS dataset')
    parser.add_argument('-p', '--fd_perc', default=10., required=False,
                        help='Directory with BIDS and derivatives datasets')
    args = parser.parse_args()
    return args


def main():

    # Parse command line arguments
    args = parse_args()
    bids_dir = Path(args.bids_dir)
    fd_perc = float(args.fd_perc)

    # Read group level QC report
    qc_file = bids_dir / 'derivatives/mriqc/group_bold.tsv'
    qc = pd.read_csv(qc_file, delimiter='\t')

    # Get problematic participants and sessions
    bad_names = qc['bids_name'][qc['fd_perc'] > fd_perc]

    # Get participants and sessions
    bad_participants = bad_names.str.extract(r'(sub-.*)_ses')[0]
    bad_sessions = bad_names.str.extract(r'(ses-.*)_task')[0]
    bad_dirs = bad_participants + '/' + bad_sessions
    bad_dirs = list(bad_dirs)

    # Check if they have been removed previously
    bad_dirs = [d for d in bad_dirs if Path(bids_dir / d).exists()]

    # Deal with bad scans
    if bad_dirs != []:

        # Remove them from the BIDS dataset
        print(f'Removing high movement scans (fd_perc > {fd_perc}):')
        print('\n'.join(bad_dirs))
        bids_ds = Dataset(bids_dir)
        bids_ds.remove(
            bad_dirs, message='Remove high movement scans from BIDS ')

        # Move them inside the sourcedata dataset
        changed_files = []
        for participant, session in zip(bad_participants, bad_sessions):
            participant = participant.replace('sub-', '')
            session = session.replace('ses-', '')
            old_dir = bids_dir / 'sourcedata' / session
            new_dir = bids_dir / 'sourcedata' / 'exclude' / session
            new_dir.mkdir(parents=True, exist_ok=True)
            old_files = old_dir.glob(participant + '*.zip')
            for old_file in old_files:
                new_file = new_dir / old_file.name
                print(f'Moving file `{old_file}` to `{new_dir}`')
                old_file.rename(new_file)
                changed_files += [old_file, new_file]

        # Save changes to BIDS dataset
        bids_ds.save(
            changed_files,
            message='Remove high movement scans from sourcedata',
            recursive=True)

    else:
        print('No high movement scans to remove.')


# Run
if __name__ == '__main__':
    main()
