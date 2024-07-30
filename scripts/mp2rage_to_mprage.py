"""
Make a normal anatomical (MPRAGE, black background) out of MP2RAGE image with noisy background.
    Adapted from https://github.com/srikash/3dMPRAGEise/blob/main/3dMPRAGEise
"""

from argparse import ArgumentParser
from pathlib import Path

import nibabel as nib
import numpy as np
from nilearn import image


def parse_args():
    """Parses arguments from the command line."""

    parser = ArgumentParser()
    parser.add_argument('-d', '--bids_dir', required=True,
                        help='Directory of the BIDS dataset')
    parser.add_argument('-sub', required=True,
                        help='Subject being processed')
    parser.add_argument('-ses', required=True,
                        help='Session being processed')
    args = parser.parse_args()
    return args


def main():
    # Parse command line arguments
    args = parse_args()
    bids_dir = Path(args.bids_dir)

    # get mp2rage image parts
    try:
        inv2_path = list(bids_dir.glob(
            f'sub-{args.sub}/ses-{args.ses}/anat/*inv-2_MP2RAGE.nii.gz'))[0]
        uni_path = list(bids_dir.glob(
            f'sub-{args.sub}/ses-{args.ses}/anat/*UNIT1.nii.gz'))[0]
    except IndexError as err:
        print('No complete MP2RAGE image found. Proceeding without.')
        print('No changes made.')
        print("Need both 'inv-2_MP2RAGE' and 'UNIT1' images. ")
        return

    # path for mprageised output
    anat_clean_outpath = bids_dir.joinpath(
        f'sub-{args.sub}', f'ses-{args.ses}', 'anat', f'sub-{args.sub}_ses-{args.ses}_acq-mprageised_T1w.nii.gz')

    # load images
    inv2 = nib.load(str(inv2_path))
    uni = nib.load(str(uni_path))

    # normalise intensity
    int_min = np.min(inv2.get_fdata())
    int_max = np.max(inv2.get_fdata())
    inv2_intnorm = image.new_img_like(
        inv2,
        (inv2.get_fdata()-int_min)/(int_max-int_min),
        copy_header=True)

    # new image is multiplied of mp2rage parts
    anat_clean = image.math_img("img1*img2", img1=uni, img2=inv2_intnorm)
    anat_clean.to_filename(str(anat_clean_outpath))

    # copy UNIT1 json file contents into json for new anat
    unit1_json = uni_path.with_suffix('').with_suffix('.json')
    anat_json = anat_clean_outpath.with_suffix('').with_suffix('.json')

    anat_json.write_text(unit1_json.read_text())

    # Run
if __name__ == '__main__':
    main()
