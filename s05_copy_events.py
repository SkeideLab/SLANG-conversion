from argparse import ArgumentParser
from fnmatch import fnmatch
from pathlib import Path
from zipfile import ZipFile
import ast

from bids import BIDSLayout
from datalad.api import Dataset
import os
import pandas as pd
import numpy as np


class LogScanAssignmentError(Exception):
    """Error that shows that the assumed assignment of logs to scans does not work
    """
    pass


class ManualChangesNeededError(Exception):
    """Error that shows that it is impossible to assign the scans and logs for this subject and
        session correctly because the number of both does not correspond. Manual intervention needed.
    """
    pass


class Unpacker:
    """Unpacker is a class to unpack log files from zips into a bids format and transform their contents to be bids-conform. 
    """

    def __init__(self, layout, bids_dir, bids_ds):
        """Initiate new instance.

        Parameters
        ----------
        layout : bids.layout
            layout object of the bids directory
        bids_dir : path
            path object of the bids directory
        bids_ds : datalad dataset
            dataset of bids directory
        """
        self.layout = layout
        self.bids_dir = bids_dir
        self.bids_ds = bids_ds
        self.lookup_events = {}
        self.lookup_logs = {}
        self.mappings = []

    def extract_logs_from_zip(self):
        """Extracts all possible events files (ending in .*sv) from zip file and saves them under bids_dir/sourcedata/session/logs/participant

        Returns
        -------
        list
            list of filenames of extracted events files
        """

        subjects = self.layout.get(target='subject', return_type='id')
        sessions = self.layout.get(target='session', return_type='id')

        extracted_files = []
        # FIXME change this to only go through subject session tuples that exist
        for session in sessions:
            for participant in subjects:
                found = False
                # pattern include optional zero for run
                pattern_leadingzero = f"*sub-{participant}_ses-{session}_*sv".casefold()
                pattern_nozero = f"*sub-{participant}_ses-{session.lstrip('0')}_*sv".casefold(
                )
                zip_files = self.bids_dir.glob(
                    f'sourcedata/{session}/{participant}_*.zip')
                save_source_dir = self.bids_dir/'sourcedata'/session/'logs'/participant
                save_source_dir.mkdir(parents=True, exist_ok=True)
                for zip_file in zip_files:
                    zip_archive = ZipFile(zip_file)
                    zip_contents = zip_archive.infolist()
                    for zipinfo in zip_contents:
                        if fnmatch(
                                zipinfo.filename.casefold(),
                                pattern_leadingzero) or fnmatch(
                                zipinfo.filename.casefold(),
                                pattern_nozero):
                            found = True
                            orig_filename = zipinfo.filename
                            zipinfo.filename = os.path.basename(
                                zipinfo.filename)
                            full_newname = str(
                                save_source_dir/zipinfo.filename)
                            if not os.path.exists(full_newname):
                                self.bids_ds.unlock(save_source_dir)
                                print(
                                    f'\nCopying `{orig_filename}` from `{zip_file}` '
                                    f'to `{save_source_dir}`', flush=True)
                                zip_archive.extract(zipinfo, save_source_dir)
                                extracted_files.append(full_newname)
                if not found:
                    print(
                        f"Error: No log files found for subject {participant} and session {session}, investigation needed", flush=True)
        return extracted_files

    def construct_lookups(self):
        """Constructs lookup dictionaries of available events and log files. Dictionaries of the form dict[session][subject][task][run]=[list of filenames]
        Populates the instance attributes lookup_events and lookup_logs. Runs are 0 if no explicit run numbering has been shown (explicit numbering is marked via acq-*dup*)
        """
        for e_file in self.events_files:
            ses = e_file.get_entities()['session']
            sub = e_file.get_entities()['subject']
            task = e_file.get_entities()['task']
            # if no dup, then numbering was produced by conversion and is not reliable
            if 'acquisition' in e_file.get_entities() and 'dup' in e_file.get_entities()['acquisition']:
                run = e_file.get_entities()['run']
            else:
                run = 0

            self.add_to_lookup('event', ses, sub, task, run, e_file)

        save_source_dir = self.bids_dir/'sourcedata'
        pattern = f"*/logs/*/*_*sv"
        log_files = save_source_dir.glob(pattern)
        for l_file in log_files:
            ses = str(l_file).split('/')[-1].split('_')[1].split('-')[1]
            sub = str(l_file).split(
                '/')[-1].split('_')[0].split('-')[1]

            # make sure sub is in the correct case as determined by already written bids scan skeleton
            for sub_events in self.lookup_events[ses]:
                if sub_events.casefold() == sub.casefold():
                    sub = sub_events

            task = self.get_taskname(l_file)
            run = self.get_runindex(l_file)

            self.add_to_lookup('log', ses, sub, task, run, l_file)

    def write_mapping(self):
        """Writes mappings of events/scan files to log files. Saved in bids-conform file bids_dir/sub-subject/ses-session/sub-subject_ses-session_scans.tsv.
        """
        # make two dicts for logs and eventsfiles
        # inside dict, first level session
        # second level subject
        # third level task
        # fourth level run: either 0 if no run info or run number
        self.construct_lookups()

        for session in self.lookup_events:
            for subject in self.lookup_events[session]:

                map_filename = str(
                    self.bids_dir / f"sub-{subject}" / f"ses-{session}" /
                    f"sub-{subject}_ses-{session}_scans.tsv")
                self.mappings.append(map_filename)
                self.mapping = pd.read_csv(map_filename, sep='\t')

                for task in self.lookup_events[session][subject]:
                    # if 'EMPRISE' in task:
                    for run in self.lookup_events[session][subject][task]:

                        try:
                            self.add_mapping(subject, session, task, run)

                        except LogScanAssignmentError as err:
                            # TODO in case of numbered runs, check possibility that the other numbers are correct
                            # because short runs might have been deleted
                            s = "WARNING: Number of scans and logs for this run not equal! Please check output carefully for correctness!\n"
                            s += '\tdeciding alignment based on timing for whole session...\n'
                            print(s, err)
                            try:
                                self.add_mapping(subject, session, task, run=0)
                            except ManualChangesNeededError as err:
                                s = "ERROR: Unequal number of logs and scans for this subject, session and/or day!\n"
                                s += '\tScans and logs need to be manually checked and deleted/added.\n'
                                print(s, err)
                            finally:
                                break
                        except ManualChangesNeededError as err:
                            s = "ERROR: Unequal number of logs and scans for this subject, session and/or day!\n"
                            s += '\tScans and logs need to be manually checked and deleted/added.\n'
                            print(s, err)
                            break

                self.bids_ds.unlock(map_filename)
                self.mapping.to_csv(map_filename, index=False, sep='\t')

    def flatten(self, dictionary):
        """Flattens a dictionary of lists of items. All items will be flattened into one list. 

        Parameters
        ----------
        dictionary : dict
            dictionary to flatten

        Returns
        -------
        list
            list of dictionary entries
        """
        return [item_file for item_list in dictionary.values() for item_file in item_list]

    def get_runindex(self, log):
        """Extract runindex from log file name. If there is no explicit run numbering, return 0.

        Parameters
        ----------
        log : str
            filepath of logfile

        Returns
        -------
        int
            number of run
        """
        run = str(log).split('/')[-1].split('_')
        if 'run-' in run[2]:
            return int(run[2].replace('run-', ''))
        else:
            return 0

    def get_scanname(self, e_file):
        """Gets path to corresponding scan from bids events file 

        Parameters
        ----------
        e_file : bids file or string
            events file

        Returns
        -------
        string
            path of corresponding scan
        """
        # convert events filename into corresponding scan filename, relative to sub/ses directory
        if not isinstance(e_file, str):
            e_file = e_file.filename
            # return '_'.join('/'.join(['func',e_file.filename]).split('_')[:-1]+['bold.nii.gz'])
        if '_events' in e_file:
            e_file = '_'.join(
                '/'.join(['func', e_file]).split('_')[:-1]+['bold.nii.gz'])
        return e_file

    def add_log(self, e_file, l_file):
        """Add log filename to scan filename in mapping df

        Parameters
        ----------
        e_file : str or bids datafile
            events file that has been identified to belong to log
        l_file : string
            log file
        """
        if not 'filename_log' in self.mapping:
            self.mapping['filename_log'] = ''

        filename_scan = self.get_scanname(e_file)

        self.mapping.loc[self.mapping['filename'] ==
                         filename_scan, 'filename_log'] = str(l_file)

    def add_mapping(self, subject, session, task, run):
        """Get mapping for all events and logs belonging to one subject,session,task,run

        Parameters
        ----------
        subject : str
            subject label
        session : str
            session label
        task : str
            task name
        run : int
            number of run we are looking at 

        Raises
        ------
        LogScanAssignmentError
            if the number of logs and events files in this run is not identical
        """
        if run == 0:
            e_file = self.flatten(self.lookup_events[session][subject][task])
            try:
                l_file = self.flatten(
                    self.lookup_logs[session.lstrip('0')][subject][task])
            except KeyError as err:
                print(err)
                print(
                    f'no log file for session {session}, subject {subject}, task {task}.')
                print('Skipping this run...')
                l_file = ['no file found']
        else:
            try:
                if not run in self.lookup_logs[str(int(session))][subject][task]:
                    raise LogScanAssignmentError()
                e_file = self.lookup_events[session][subject][task][run]
                l_file = self.lookup_logs[session.lstrip(
                    '0')][subject][task][run]
            except KeyError:
                raise ManualChangesNeededError()

        if len(e_file) > 1 or len(l_file) > 1:
            scan_log = self.resolve_times(
                subject, session, task, run, e_file, l_file)
        else:
            scan_log = [(e_file[0], l_file[0])]

        for (s_file, l_file) in scan_log:
            self.add_log(s_file, l_file)

    def resolve_times(self, subject, session, task, run, e_files, l_files):
        """Connect events and log files without looking at not-reliable run-numbers or within a run, based on timing.

        Parameters
        ----------
        subject : str
            subject label
        session : str
            session label
        task : str
            task name
        run : int
            number of run we are looking at
        e_files : list of bids files
            event files for this bundle
        l_files : list of strings
            log files for this bundle

        Returns
        -------
        list of tuples of scan_file,log_file
            list of tuples of scan_file,log_file

        Raises
        ------
        ManualChangesNeededError
            it is not possible to solve this problem within the program.
        LogScanAssignmentError
            run numbers will be ignored next time
        """
        if not len(e_files) == len(l_files):
            s = f"\tsubject: {subject} session: {session} task: {task} run: {run}\n"
            s += f"\tscan files:\n"
            for item in e_files:
                s += f"\t {str(item)}\n"
            s += f"\tlog files:\n"
            for item in l_files:
                s += f"\t {str(item)}\n"
            #s += f"\tscan files: {[str(item) for item in e_files]}\n"
            #s += f"\tlog files: {[str(item) for item in l_files]}\n"
            s += '.'
            if run == 0:
                raise ManualChangesNeededError(s)
            else:
                raise LogScanAssignmentError(s)

         # treat days individually
        s_files = [self.get_scanname(e_file) for e_file in e_files]
        runs = self.mapping.loc[self.mapping['filename'].isin(s_files)]
        runs = runs.sort_values('acq_time')

        log_times = []

        # get creation time out of logs
        for csv_log in l_files:
            time = self.get_logtime(csv_log)
            log_times.append(time)

        # sort logs and acquisitions by time so that the ranks of each correspond
        logs = pd.DataFrame({'csv_log': l_files, 'log_times': log_times})
        logs.sort_values('log_times', inplace=True)
        logs.reset_index(drop=True, inplace=True)
        logs['acq_day'] = self.get_day(logs['log_times'])

        runs['acq_day'] = self.get_day(runs['acq_time'])
        runs.reset_index(drop=True, inplace=True)
        days_runs = np.unique(runs['acq_day'])
        assigned = []
        for day in days_runs:
            logs_day = logs[logs['acq_day'] == day]
            runs_day = runs[runs['acq_day'] == day]
            if runs_day.shape[0] != logs_day.shape[0]:
                s = f"\tsubject: {subject} session: {session} task: {task} run: {run} day: {days_runs}\n"
                s += f"\tscan files: {[str(item) for item in e_files]}\n"
                s += f"\tlog files: {[str(item) for item in l_files]}\n"
                s += '.'
                raise ManualChangesNeededError(s)
            else:
                assigned.extend([(run, str(log)) for run, log in zip(
                    runs_day['filename'], logs_day['csv_log'])])

        return assigned

    def get_day(self, time_series):
        return time_series.str.split('T').str[0]

    def get_logtime(self, csv_log):

        # time format of runs: 2022-04-30T10:17:15.0
        # of logs: 2022_04_30_1021
        desc = csv_log.name.split('.')[0].split('_')
        offset = 0
        if desc[-1] == 'events':
            offset = 1
        year = desc[-(4+offset)]
        month = desc[-(3+offset)]
        day = desc[-(2+offset)]
        time = desc[-(1+offset)]
        time = f"{time[0:2]}:{time[2:4]}:00.000000"

        return f"{year}-{month}-{day}T{time}"

    def get_taskname(self, log):
        """Gets taskname from log filename

        Parameters
        ----------
        log : str
            filename of log file

        Returns
        -------
        string
            task for this log
        """
        filename = str(log).split('/')[-1].split('_')

        # if we also have a run identifier
        if 'run-' in filename[2]:  # filename[2][:4] == 'run-':
            # take second identifier
            filename = filename[3]
        else:
            # else take first identifier
            filename = filename[2]

        # remove hyphens and return
        return filename.replace('-', '')

    def extract_onsets(self):
        """for every written mapping of events file to log file, adapt copy content of logfile to events file 
        """
        for map_filename in self.mappings:
            mapping = pd.read_csv(map_filename, sep='\t')
            if 'filename_log' in mapping:
                for filename_scan, filename_log in zip(
                        mapping['filename'],
                        mapping['filename_log']):
                    filename_events = self.get_events_filename(
                        filename_scan, map_filename)
                    self.transform_log_content(filename_events, filename_log)
            else:
                print('no logs found for ', str(map_filename), flush=True)

    def get_events_filename(self, filename_scan, map_filename):
        """get filename of events file from scan filename

        Parameters
        ----------
        filename_scan : str
            path to scan file
        map_filename : str
            path to mapping file

        Returns
        -------
        str
            path to events file
        """
        tmp = os.path.join(os.sep.join(
            map_filename.split(os.sep)[:-1]), filename_scan)
        tmp = f"{'_'.join(tmp.split('_')[:-1])}_events.tsv"
        return tmp

    def extract_priming_onsets(self, log_filename, delim):

        onsets = pd.read_csv(log_filename, sep=delim, converters={
                             'target.started': make_list, 'target.stopped': make_list})

        # only deal with stim trials
        mask = onsets['mod_prime'] != 'pause'
        # multiple stimuli start times per trial in list
        onset = onsets.loc[mask, 'target.started'].apply(np.min)
        duration = onsets.loc[mask, 'target.stopped'].apply(
            np.min) - onsets.loc[mask, 'target.started'].apply(np.min)

        # prime trials where numerosity of prime and target is identical
        mask_prime = onsets.loc[mask,
                                'num_prime'] == onsets.loc[mask, 'num_target']
        events = pd.DataFrame(
            {'onset': onset, 'duration': duration, 'trial_type': 'nonprime'})
        events.loc[mask_prime, 'trial_type'] = 'prime'
        events['modality'] = onsets.loc[mask, 'mod_prime'] + \
            '_' + onsets.loc[mask, 'mod_target']

        return events

    # transform content

    def transform_log_content(self, events_filename, log_filename):
        """transform log content to be bids conform

        Parameters
        ----------
        events_filename : str
            path to events file
        log_filename : str
            path to log file

        Returns
        -------
        str
            path to events filename (changed file)
        """
        if not os.path.exists(str(log_filename)):
            return
        # prepare delimiter for .csv and .tsv
        delim = '\t'
        if log_filename.split('.')[-1] == 'csv':
            delim = ','

        onsets = pd.read_csv(log_filename, sep=delim)

        # process columns

        if 'priming' in self.layout.get_tasks():
            events = self.extract_priming_onsets(log_filename, delim)
        else:
            events_list = []
            if 'onset' in onsets:
                events_list.append(onsets['onset'])
            else:
                events_list.append(onsets['t_start'].round(decimals=2))

            if 't_stop' in onsets and 't_start' in onsets:
                events_list.append(
                    (onsets['t_stop']-onsets['t_start']).round(decimals=2))
            else:
                events_list.append(onsets['duration'])

            if 'trial_type' in onsets:
                events_list.append(onsets['trial_type'])
            else:
                if 'num' in onsets:
                    events_list.append(
                        onsets['num'].astype(str) + '_' +
                        onsets['mod'].str.replace(' |\'', '', regex=True))
                elif 'condition' in onsets and 'truth' in onsets:
                    events_list.append(onsets['condition']+'_'+onsets['truth'])

            events = pd.concat(events_list, axis=1, keys=[
                'onset', 'duration', 'trial_type'])
            # remove pause information
            events = events[~events['trial_type'].str.contains('pause')]

        self.bids_ds.unlock(os.path.dirname(events_filename))
        # save
        events.to_csv(events_filename, sep='\t', index=False)
        return events_filename

    def find_zipfile(self, zip_files, pattern):
        """Find log in zipfile based on pattern

        Parameters
        ----------
        zip_files : list 
            list of zip file names
        pattern : str
            pattern with wildcard for logs

        Returns
        -------
        list
            list of dictionaries with 'zip': zipfile wherein a log was found, 'zipinfo': exact file inside zip that is a log
        """
        # for slang and localizers
        file_list = []
        for zip_file in zip_files:
            zip = ZipFile(zip_file)
            zipinfos = zip.infolist()
            for zipinfo in zipinfos:

                if fnmatch(zipinfo.filename, pattern):
                    file_list.append({'zip': zip, 'zipinfo': zipinfo})

        # for emprise, read mapping
        # READ MAPPING
        # CONVERT LOGS
        # SAVE LOGS

        return file_list

    def has_multirun_design(self):
        for task in self.layout.get_tasks():
            if 'harvey' in task or 'EMPRISE' in task or 'priming' in task:
                return True
        return False

    def add_to_lookup(self, category, ses, sub, task, run, c_file):
        """Adds files to the lookup dictionaries at the correct place

        Parameters
        ----------
        category : str
            'log' or 'event'
        ses : str
            session
        sub : str
            subject
        task : str
            task name
        run : int
            run number
        c_file : str or bids datafile
            file to be saved in the lookup
        """
        if category == 'event':
            c_dict = self.lookup_events
        elif category == 'log':
            c_dict = self.lookup_logs

        if not ses in c_dict:
            c_dict[ses] = {}
        if not sub in c_dict[ses]:
            c_dict[ses][sub] = {}
        if not task in c_dict[ses][sub]:
            c_dict[ses][sub][task] = {}
        if not run in c_dict[ses][sub][task]:
            c_dict[ses][sub][task][run] = []

        c_dict[ses][sub][task][run].append(c_file)


def make_list(input):
    if '[' in input:
        return ast.literal_eval(input)
    else:
        return input


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
    unpacker = Unpacker(BIDSLayout(bids_dir), bids_dir, bids_ds)
    unpacker.events_files = unpacker.layout.get(
        suffix='events', extension='tsv')
    print(
        f"subjects: {unpacker.layout.get(target='subject',return_type='id')}",
        flush=True)
    print(
        f"sessions: {unpacker.layout.get(target='session',return_type='id')}",
        flush=True)
    # Search for correspdonding events files created by PsychoPy in the zips
    extracted_files = []
    # HACK for emprise, to integrate with other part
    if unpacker.has_multirun_design():
        extracted_files = unpacker.extract_logs_from_zip()
        unpacker.write_mapping()
        unpacker.extract_onsets()

        Dataset(unpacker.bids_dir /
                'sourcedata').save(message='Copy `events.tsv` files to BIDS')
        unpacker.bids_ds.save(message='Copy `events.tsv` files to BIDS')

    else:

        for events_file in unpacker.events_files:
            participant = events_file.subject
            session = events_file.session
            zip_files = bids_dir.glob(
                f'sourcedata/{session}/{participant}_*.zip')

            match_files = unpacker.find_zipfile(zip_files, pattern)

            assert len(match_files) == 1, \
                f'Found multiple events files matching `{events_file}`'

            zipinfo = match_files[0]['zipinfo']
            zip = match_files[0]['zip']

            orig_filename = zipinfo.filename
            zipinfo.filename = events_file.filename
            print(f'\nCopying `{orig_filename}` from `{zip.filename}` '
                  f'to `{events_file.path}`')
            bids_ds.unlock(events_file.dirname)
            zip.extract(zipinfo, events_file.dirname)
            extracted_files.append(events_file.path)

        # Save changes in the Dataset
        bids_ds.save('sub-*/', message='Copy `events.tsv` files to BIDS')

        # TODO worry about later:
        # 1 session split into multiple days
        # incomplete runs under 10
    print('', flush=True)


# Run
if __name__ == '__main__':
    main()
