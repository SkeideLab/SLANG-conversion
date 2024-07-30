import json
from pathlib import Path
import warnings
import re

# fill 'intended for' fields in json of fieldmaps
POPULATE_INTENDED_FOR_OPTS = {
    'matching_parameters': ['ImagingVolume', 'Shims'],
    'criterion': 'Closest'
}


def infotodict(seqinfo):
    """Used by heudiconv to detect relevant scans based on heuristics."""

    # Prepare empty dict for BIDS file names (keys) and sequences (values)
    info = {}

    # ------------- Prepare heuristics based on run_params.json file--------------------------------
    # Load heuristic params
    run_params = prepare_options()

    # Make heudiconv overwrite previous runs if requested
    item = '{item}'
    if 'latest_run_only' in run_params:
        if run_params['latest_run_only']:
            item = '1'

    heuristics_dict_func = {}
    heuristics_list_t1w = []
    heuristics_list_mp2rage = []
    if 't1w_heuristic' in run_params:
        # Detect T1w scans
        heuristic = run_params['t1w_heuristic']
        # Make a list with possible heuristics for this sequence type
        include_acq_t1w, heuristics_list_t1w = make_heuristics_list(heuristic)
    if 'mp2rage_heuristic' in run_params:
        heuristic = run_params['mp2rage_heuristic']
        # Make a list with possible heuristics for this sequence type
        _, heuristics_list_mp2rage = make_heuristics_list(heuristic)
    if 'func_heuristic' in run_params:
        for task, heuristic in run_params['func_heuristic'].items():
            # Make a list with possible heuristics for this task
            include_acq, heuristics_list = make_heuristics_list(heuristic)
            heuristics_dict_func[task] = {
                'include_acq': include_acq, 'heuristics_list': heuristics_list}

    # Detect scans based on the provided heuristics
    for s in seqinfo:

        # -------- T1w SCANS --------------------------------------------------
        # Each item in this list is a heuristic with only 1 series description
        # The heuristic is a decomposed dict (items() list)
        # list of tuples, each tuple being a (key, value) pair of the dict
        for sub_heuristic in heuristics_list_t1w:

            # Check if current scan meets all conditions in the T1w heuristic
            if does_seq_match_heu(s, sub_heuristic):
                # Prepare a BIDS acquisition descriptor
                if include_acq_t1w:
                    acq = s.series_description
                    # Remove punctuation marks and common irrelevant descriptors
                    for to_remove in ['t1', '_', '-', '.']:
                        acq = acq.replace(to_remove, '')
                    acq = f"_acq-{acq}"
                else:
                    acq = ''

                t1w = create_key(  # Create BIDS file name and extension
                    f"{{bids_subject_session_dir}}/anat/{{bids_subject_session_prefix}}{acq}_run-{item}_T1w")
                info.setdefault(t1w, []).append({'item': s.series_id})

        # ------------- FUNCTIONAL SCANS --------------------------------------
        # Detect func scans separately for each task
        for task in heuristics_dict_func.keys():
            heuristics_list = heuristics_dict_func[task]['heuristics_list']
            include_acq = heuristics_dict_func[task]['include_acq']
            # Each item in this list is a heuristic with only 1 series
            # description; the heuristic is a decomposed dict (items() list)
            for sub_heuristic in heuristics_list:

                # Check if current scan meets all conditions in the task
                # heuristic
                if does_seq_match_heu(s, sub_heuristic):
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

                    # in case of phase functional images eg. from 7t
                    # normal images have 'M' for magnitude
                    phase = ''
                    if 'P' in s.image_type:
                        phase = '_part-phase'
                    # Additionally, functional scans must have > 10 volumes
                    # (files)
                    if s.series_files < 10:
                        if acq:
                            acq += 'short'
                        else:
                            acq = "_acq-short"

                    func = create_key(  # Create BIDS file name and extension
                        f"{{bids_subject_session_dir}}/func/{{bids_subject_session_prefix}}_task-{task}{acq}_run-{run_index}{phase}_bold")
                    info.setdefault(func, []).append({'item': s.series_id})

        # Detect fmap scans separately for each direction
        for direction, heuristic in run_params['fmap_heuristic'].items():
            # Check if current scan meets all conditions in the dir heuristic
            if does_seq_match_heu(s, heuristic.items()):
                fmap = create_key(  # Create BIDS file name and extension
                    f"{{bids_subject_session_dir}}/fmap/{{bids_subject_session_prefix}}_dir-{direction}_run-{item}_epi")
                info.setdefault(fmap, []).append({'item': s.series_id})

        # -------- mp2rage SCANS --------------------------------------------------
        # Detect mp2rage scans
        if 'mp2rage_heuristic' in run_params:

            # Each item in this list is a heuristic with only 1 series description
            # The heuristic is a decomposed dict (items() list)
            for sub_heuristic in heuristics_list_mp2rage:
                # Check if current scan meets all conditions in the T1w heuristic
                if does_seq_match_heu(s, sub_heuristic):
                    if 'T1 MAP' in s.image_type:
                        suffix = 'T1map'
                        path = 'derivatives/scanner/'
                        # we currently discard this file
                        # would go into 'derivatives' folder
                        # (before bidsonym step)
                        continue
                    elif 'UNI' in s.image_type:
                        suffix = 'UNIT1'
                    else:

                        if 'INV2' in s.series_description:
                            inv = '2'
                        elif 'INV1' in s.series_description:
                            inv = '1'
                        else:
                            inv = ''
                        suffix = f"inv-{inv}_MP2RAGE"

                    t1w = create_key(  # Create BIDS file name and extension
                        f"{{bids_subject_session_dir}}/anat/{{bids_subject_session_prefix}}_run-{item}_{suffix}")
                    info.setdefault(t1w, []).append({'item': s.series_id})

    return info


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
        for param in ['t1w_heuristic', 'func_heuristic', 'fmap_heuristic', 'mp2rage_heuristic']:
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


def does_seq_match_heu(s, sub_heuristic):
    """Determines if the sequence attribute match the heuristic attributes

    Parameters
    ----------
    s : dict
        dict produced by heudiconv with info about 1 sequence
    sub_heuristic : dict
        the sequence attributes/conditions that need to be satisfied for this modality/sequence type

    Returns
    -------
    boolean
        True if `s` satisfies the constraints in `sub_heuristic`, else False
    """
    return all([
        getattr(s, sequence_attribute) == matching_value
        for sequence_attribute, matching_value
        in sub_heuristic])


def make_heuristics_list(heuristic):
    """Make a list of multiple, independent heuristic constraints 

    Parameters
    ----------
    heuristic : dict
        with keys type of attribute and values either a list or single value 
        that has to be matched. If list, the values apply independently, that is one has to be matched.

    Returns
    -------
    boolean, list
        True if we have multiple different kinds of sequence that match this 
        type, False else. List with independent heuristics with only one value per key.
    """
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
    return include_acq, heuristics_list


def create_key(template, outtype=('nii.gz',), annotation_classes=None):
    """Used by heudiconv to create BIDS file names and extensions."""

    if template is None or not template:
        raise ValueError('Template must be a valid format string')

    return template, outtype, annotation_classes