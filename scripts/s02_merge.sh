#!/bin/bash -l

# Fail whenever something is fishy; use -x to get verbose logfiles
set -e -u -x

# Parse arguments from the job scheduler as variables
bids_dir=$1
shift
merge_job_ids=("$@")

# Activate conda environment
module load anaconda/3/2023.03
conda activate SLANG

# Go into the Dataset directory
cd "$bids_dir"

# Create empty strings
local_branches=""
remote_branches=""
unsucessful_jobs=""

# Check which jobs finished succesfully
for job_id in "${merge_job_ids[@]}"; do
    log_file="$bids_dir/code/logs/slurm-$job_id-*.out"
    if grep -Fxq "SUCCESS" $log_file; then
        local_branches+=" output/job-$job_id"
        remote_branches+=" job-$job_id"
    else
        unsucessful_jobs+=" $job_id"
    fi
done

# Merge and delete successful branches
if [ -n "$local_branches" ]; then

    # Merge branches into the BIDS dataset
    datalad update --sibling output
    git merge -m "Merge batch job results" $local_branches
    git annex fsck --fast -f output-storage
    datalad get -d . -s output -s output-storage
    git push --delete output $remote_branches

    # Merge branches into the derivatives sub-dataset
    cd derivatives
    datalad update --sibling output
    git merge -m "Merge batch job results" $local_branches
    git annex fsck --fast -f output-storage
    datalad get -d . -s output -s output-storage
    git push --delete output $remote_branches
    datalad save -d . -m "Add QC reports to derivatives" derivatives/mriqc

fi

# Warn about unsucessful branches
if [ -n "$unsucessful_jobs" ]; then
    echo "WARNING: Not merging unsuccessful batch jobs $unsucessful_jobs." \
        "Please check their log files and Dataset clones."
fi

# And we're done
echo SUCCESS
