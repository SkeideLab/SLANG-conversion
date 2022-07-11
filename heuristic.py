import json
from pathlib import Path
import warnings
import re


def prepare_options():
    """Reads in the file `<my_study>/code/run_params.json` and checks for
    the presence of relevant parameters to run the heudiconv heuristic.

    Returns
    -------
    run_params: dict
        Contains the parameters for this study. Must include one or more of
        't1w_heuristic', 'func_heuristic', 'fmap_heuristic' 'latest_run_only'.
    """

    # Read the JSON file
    with open(f"{Path(__file__).parent.resolve()}/run_params.json", 'r') as fp:
        run_params = json.load(fp)

        # Check for the heuristic parameters
        found_one = False
        for param in ['t1w_heuristic', 'func_heuristic', 'fmap_heuristic']:
            if param not in run_params:
                warnings.warn(
                    f"Parameter '{param}' not found in `run_params.json`."
                    " Heudiconv won't be able to detect this type of scan!")
            else:
                found_one = True

        # Exit if we didn't find a single heuristic parameter
        assert found_one, (
            "`run_params.json` must include at least one '*_heuristic'"
            " parameter or heudiconv won\'t be able to detect any scans!")

        # Check for the `latest_run_only` parameter
        if 'latest_run_only' not in run_params:
            warnings.warn(
                "Didn't find 'latest_run_only' parameter in `run_params.json`."
                " Assuming you want to retain all runs.")

        return run_params


def infotodict(seqinfo):
    """Used by heudiconv to detect relevant scans based on heuristics."""

    # Load heuristic params
    run_params = prepare_options()

    # Make heudiconv overwrite previous runs if requested
    item = '{item}'
    if 'latest_run_only' in run_params:
        if run_params['latest_run_only']:
            item = '1'

    # Prepare empty dict for BIDS file names (keys) and sequences (values)
    info = {}

    # Detect scans based on the provided heuristics
    for s in seqinfo:

        # -------- T1w SCANS --------------------------------------------------
        # Detect T1w scans
        heuristic = run_params['t1w_heuristic']
        # Make a list with possible heuristics for this task
        include_acq = False
        heuristics_list = []
        if 'series_description' in heuristic:
            # If we have multiple series descriptions
            if isinstance(heuristic['series_description'], list):
                # We want to have a BIDS acquisition descriptor to distinguish
                # the sequences for same task
                include_acq = True
                # For each series description in list, make small heuristic
                # dict with series description as single string, unchanged
                # otherwise
                h = heuristic.copy()
                for s_description in heuristic['series_description']:
                    h['series_description'] = s_description
                    # Save this new heuristic dict decomposed as list without
                    # relation to dict
                    heuristics_list.append(list(h.items()))
            else:
                # If there is only 1 series_description, then just decompose
                # the one heuristic
                heuristics_list.append(list(heuristic.items()))

        # Each item in this list is a heuristic with only 1 series description
        # The heuristic is a decomposed dict (items() list)
        for sub_heuristic in heuristics_list:
            # Prepare a BIDS acquisition descriptor
            if include_acq:
                acq = s.series_description
                # Remove punctuation marks and common irrelevant descriptors
                for to_remove in ['t1', '_', '-', '.']:
                    acq = acq.replace(to_remove, '')
                acq = f"_acq-{acq}"
            else:
                acq = ''

            # Check if current scan meets all conditions in the T1w heuristic
            if all([getattr(s, sequence_attribute) == matching_value
                    for sequence_attribute, matching_value in sub_heuristic]):
                t1w = create_key(  # Create BIDS file name and extension
                    f"{{bids_subject_session_dir}}/anat/{{bids_subject_session_prefix}}{acq}_run-{item}_T1w")
                info.setdefault(t1w, []).append({'item': s.series_id})

        # ------------- FUNCTIONAL SCANS --------------------------------------
        # Detect func scans separately for each task
        for task, heuristic in run_params['func_heuristic'].items():

            include_acq = False
            # Make a list with possible heuristics for this task
            heuristics_list = []
            if 'series_description' in heuristic:
                # If we have multiple series descriptions
                if isinstance(heuristic['series_description'], list):
                    # We want to have a bids acquisition descriptor to
                    # distinguish the sequences for same task
                    include_acq = True
                    # For each series description in list, make small heuristic
                    # dict with series description as single string, unchanged
                    # otherwise
                    h = heuristic.copy()
                    for s_description in heuristic['series_description']:
                        h['series_description'] = s_description
                        # Save this new heuristic dict decomposed as list
                        # without relation to dict
                        heuristics_list.append(list(h.items()))
                else:
                    # If there is only 1 series_description, then just
                    # decompose the one heuristic
                    heuristics_list.append(list(heuristic.items()))

            # Each item in this list is a heuristic with only 1 series
            # description; the heuristic is a decomposed dict (items() list)
            for sub_heuristic in heuristics_list:
                # Prepare a BIDS acquisition descriptor
                if include_acq:
                    acq = s.series_description
                    # Remove punctuation marks and common irrelevant
                    # descriptors
                    for to_remove in ['cmrr', 'mbep2d', 'bold', '_', '-', '.']:
                        acq = acq.replace(to_remove, '')
                    acq = f"_acq-{acq}"
                else:
                    acq = ''

                # Try to extract run index
                # Cut series description into parts, check if part is single
                # number, if yes that is the run index
                run_number = [
                    match.group()
                    for match
                    in
                    [re.match('^\d$', part)
                     for part in s.series_description.split('_')] if match]
                if run_number:
                    run_number = run_number[0]
                    run_index = run_number

                    # Identifier for originally numbered runs: will have 'dup'
                    # in acquisition; remove other acq info, there will only
                    # ever be one version of the numbered runs
                    acq = f"_acq-dup{item}"

                else:
                    run_index = item

                # Check if current scan meets all conditions in the task
                # heuristic
                if all([getattr(s, sequence_attribute) == matching_value
                        for sequence_attribute, matching_value in
                        sub_heuristic]):
                    # Additionally, functional scans must have > 10 volumes
                    # (files)
                    if s.series_files < 10:
                        if acq:
                            acq += 'short'
                        else:
                            acq = "_acq-short"

                    func = create_key(  # Create BIDS file name and extension
                        f"{{bids_subject_session_dir}}/func/{{bids_subject_session_prefix}}_task-{task}{acq}_run-{run_index}_bold")
                    info.setdefault(func, []).append({'item': s.series_id})

        # Detect fmap scans separately for each direction
        for direction, heuristic in run_params['fmap_heuristic'].items():
            # Check if current scan meets all conditions in the dir heuristic
            if all(
                [getattr(s, sequence_attribute) == matching_value
                 for sequence_attribute, matching_value in heuristic.items()]):
                fmap = create_key(  # Create BIDS file name and extension
                    f"{{bids_subject_session_dir}}/fmap/{{bids_subject_session_prefix}}_dir-{direction}_run-{item}_epi")
                info.setdefault(fmap, []).append({'item': s.series_id})

    return info


def create_key(template, outtype=('nii.gz',), annotation_classes=None):
    """Used by heudiconv to create BIDS file names and extensions."""

    if template is None or not template:
        raise ValueError('Template must be a valid format string')

    return template, outtype, annotation_classes
