#!/bin/bash

# Fail whenever something is fishy; use -x to get verbose logfiles
set -e -u -x

# Parse arguments from the job scheduler as variables
bids_dir=$1
bids_remote=$2
deriv_remote=$3
participant=$4
session=$5
fd_thres=$6

# Enable use of Singularity containers
module load singularity

# Create temporary location
tmp_dir="/ptmp/$USER/tmp"
mkdir -p "$tmp_dir"

# Clone the BIDS dataset
# Flock makes sure that pull and push does not interfere with other jobs
lockfile="$bids_dir/.git/datalad_lock"
job_dir="$tmp_dir/ds_job_$SLURM_JOB_ID"
flock --verbose "$lockfile" datalad clone "$bids_dir" "$job_dir"
cd "$job_dir"

# Announce the clone to be temporary
git submodule foreach --recursive git annex dead here

# Checkout unique branches in both datasets
git checkout -b "job-$SLURM_JOB_ID"
datalad get --no-data derivatives
git -C derivatives checkout -b "job-$SLURM_JOB_ID"

# Make sure that BIDS metadata from previous sessions is available
datalad --on-failure ignore get --dataset . \
  sub-*/ses-*/*.json \
  sub-*/ses-*/*/*.json \
  derivatives/mriqc

# Create temporary sub-directory for unzipped DICOMs
dicom_dir=".tmp/dicom_dir"
mkdir -p "$dicom_dir"

# Unzip DICOMs and put them into a tar file
# This helps with performance of Datalad and heudiconv
zip_files="sourcedata/${session}/${participant}_*.zip"
tar_file=".tmp/dicoms.tar"
datalad run \
  --input "$zip_files" \
  --output "$tar_file" \
  --message "Convert zipped DICOMs to tar" \
  --explicit \
  "unzip -jnqd $dicom_dir '$zip_files' && \
tar -cf $tar_file $dicom_dir/* && \
rm -rf $dicom_dir/"

# Convert DICOMs to BIDS
sub_ses_dir="sub-$participant/ses-$session/"
datalad containers-run \
  --container-name "code/containers/repronim-reproin" \
  --input "$tar_file" \
  --output "$sub_ses_dir" \
  --message "Convert DICOMs to BIDS" \
  --explicit "\
--files {inputs} \
--subjects $participant \
--outdir $job_dir \
--heuristic code/heuristic.py \
--ses $session \
--bids \
--overwrite \
--minmeta \
--dcmconfig code/dcmconfig.json"

# Clean up tar file
git rm -rf "$tar_file"
datalad save --message "Cleanup temporary files" "$tar_file"

# Delete short runs so they don't get processed with mriqc
rm -f sub-"$participant"/ses-"$session"/func/*_acq-*short*
datalad save --message "Delete short scans" sub-"$participant"/ses-"$session"/

# Convert any MP2RAGE scans to MPRAGE
# Note that this will not affect datasets without MP2RAGE scans
sub_ses_anat_dir="sub-$participant/ses-$session/anat"
datalad run \
  --assume-ready inputs \
  --input "$sub_ses_anat_dir" \
  --output "$sub_ses_anat_dir" \
  --message "Convert any MP2RAGE scans to MPRAGE" \
  --explicit "\
python code/mp2rage_to_mprage.py \
--bids_dir $job_dir \
-sub $participant \
-ses $session" 

# Remove all other sessions before defacing and create directory if it doesn't exist yet
# This is necessary because bidsonym has no --session_label flag
mkdir -p "$sub_ses_dir"
tmp_ses_dir=".tmp_ses_dir"
mv "$sub_ses_dir" "$tmp_ses_dir"
rm -rf sub-"$participant"/ses-*/
mv "$tmp_ses_dir" "$sub_ses_dir"

# Defacing
datalad containers-run \
  --container-name "code/containers/bids-bidsonym" \
  --input "$sub_ses_dir" \
  --output "$sub_ses_dir" \
  --message "Deface anatomical image" \
  --explicit "\
$job_dir participant \
--participant_label $participant \
--deid pydeface \
--brainextraction bet \
--bet_frac 0.5 \
--skip_bids_validation"

# Push large files to the RIA stores
# Does not need a lock, no interaction with Git
datalad push --dataset . --to output-storage

# Push to output branches
# Needs a lock to prevent concurrency issues
git remote add outputstore "$bids_remote"
flock --verbose "$lockfile" git push outputstore

# Create output directory for quality control
mriqc_dir="derivatives/mriqc/"
mkdir -p "$mriqc_dir"

# Participant level quality control
datalad containers-run \
  --container-name "code/containers/bids-mriqc" \
  --input "$sub_ses_dir" \
  --output "$mriqc_dir" \
  --message "Create participant level quality reports" \
  --explicit "\
$job_dir $mriqc_dir participant \
--participant-label $participant \
--session-id $session \
--nprocs $SLURM_CPUS_PER_TASK \
--mem $((SLURM_MEM_PER_NODE / 1024)) \
--float32 \
--work-dir $JOB_TMPDIR \
--verbose-reports \
--no-sub \
--fd_thres $fd_thres"

# Push large files to the RIA stores
# Does not need a lock, no interaction with Git
datalad push --dataset derivatives --to output-storage

# Push to output branches
# Needs a lock to prevent concurrency issues
git -C derivatives remote add outputstore "$deriv_remote"
flock --verbose "$lockfile" git -C derivatives push outputstore

# Clean up everything
chmod -R +wrx "$job_dir"
rm -rf "$job_dir"

# And we're done
echo SUCCESS
