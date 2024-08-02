from argparse import ArgumentParser
from fnmatch import fnmatch
from pathlib import Path
from zipfile import ZipFile

from bids import BIDSLayout
from datalad.api import Dataset


def parse_args():
    """Parses arguments from the command line."""

    parser = ArgumentParser()
    parser.add_argument('-d', '--bids_dir', required=True,
                        help='Directory of the BIDS dataset')
    parser.add_argument('-p', '--pattern', default='*_events.tsv',
                        required=False,
                        help='Pattern to match the events file from PsychoPy')
    args = parser.parse_args()
    return args


def main():

    # Parse command line arguments
    args = parse_args()
    bids_dir = Path(args.bids_dir)
    pattern = str(args.pattern)

    # Get Datalad dataset
    bids_ds = Dataset(bids_dir)

    # Get all dummy events files from the BIDS structure
    layout = BIDSLayout(bids_dir)
    events_files = layout.get(suffix='events', extension='tsv')

    # Search for correspdonding events files created by PsychoPy in the zips
    extracted_files = []
    for events_file in events_files:
        participant = events_file.subject
        session = events_file.session
        zip_files = bids_dir.glob(f'sourcedata/{session}/{participant}_*.zip')
        for zip_file in zip_files:
            zip = ZipFile(zip_file)
            zipinfos = zip.infolist()
            for zipinfo in zipinfos:
                if fnmatch(zipinfo.filename, pattern):
                    orig_filename = zipinfo.filename
                    zipinfo.filename = events_file.filename
                    print(f'\nCopying `{orig_filename}` from `{zip_file}` '
                          f'to `{events_file.path}`')
                    bids_ds.unlock(events_file.dirname)
                    zip.extract(zipinfo, events_file.dirname)
                    extracted_files.append(events_file.path)

    # Test if multiple log files where found for the same participant/session
    for file in extracted_files:
        assert extracted_files.count(file) == 1, \
            f'Found multiple events files matching `{file}`'

    # Save changes in the Dataset
    bids_ds.save(extracted_files, message='Copy `events.tsv` files to BIDS')


# Run
if __name__ == '__main__':
    main()
