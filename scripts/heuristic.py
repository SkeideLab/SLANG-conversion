import json
from pathlib import Path


def get_params():
    """Reads in the file <my_study>/code/run_params.json to set parameters for 
    the data processing run.

    Returns
    -------
    run_params: dict    
        Contains the parameters for this run. Always includes
        'latest_run_only', 'task_name','t1w_pattern'.
    """

    code_dir = Path(__file__).resolve().parents[1]
    params_file = code_dir / 'run_params.json'
    with open(params_file, 'r') as f:
        run_params = json.load(f)

        compulsory_params = ['latest_run_only',
                             't1w_heuristic',
                             'func_heuristic']

        # Check that the necessary parameters are now available in run_params
        for c_param in compulsory_params:
            assert c_param in run_params, \
                f'Compulsory parameter {c_param} is not in `run_params.json`'

        return run_params


def infotodict(seqinfo):

    # Load conversion params
    run_params = get_params()

    # Overwrite previous runs if necessary
    item = '1' if run_params['latest_run_only'] else '{item}'

    # Prepare empty dict for BIDS file names (keys) and sequences (values)
    info = {}

    # Detect scans based on the provided heuristics
    for s in seqinfo:

        # Detect T1w scans
        heuristic = run_params['t1w_heuristic']
        if all([getattr(s, k) == v for k, v in heuristic.items()]):
            t1w = create_key(f"{{bids_subject_session_dir}}/anat/"
                             f"{{bids_subject_session_prefix}}_run-{item}_T1w")
            info.setdefault(t1w, []).append({'item': s.series_id})

        # Detect func scans for each task
        for task, heuristic in run_params['func_heuristic'].items():
            if all([getattr(s, k) == v for k, v in heuristic.items()]):
                func = create_key(f"{{bids_subject_session_dir}}/func/"
                                  f"{{bids_subject_session_prefix}}_task-{task}_run-{item}_bold")
                info.setdefault(func, []).append({'item': s.series_id})

    return info


def create_key(template, outtype=('nii.gz',), annotation_classes=None):

    if template is None or not template:
        raise ValueError('Template must be a valid format string')

    return template, outtype, annotation_classes
