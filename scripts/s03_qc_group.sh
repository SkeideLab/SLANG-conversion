#!/bin/bash -l

# Fail whenever something is fishy; use -x to get verbose logfiles
set -e -u -x

# Parse arguments from the job scheduler as variables
bids_dir=$1
fd_thres=$2

# Enable use of Singularity containers
module load apptainer

# Activate conda environment
module load anaconda/3/2023.03
conda activate slang

# Go into the BIDS dataset
cd "$bids_dir"

# Participant level quality control
mriqc_dir="derivatives/mriqc"
datalad containers-run \
  --container-name "code/containers/bids-mriqc" \
  --input . \
  --output "$mriqc_dir" \
  --message "Create group level quality reports" \
  --explicit "\
$bids_dir $mriqc_dir group \
--nprocs $SLURM_CPUS_PER_TASK \
--mem $((SLURM_MEM_PER_NODE / 1024)) \
--float32 \
--work-dir /tmp/ \
--verbose-reports \
--no-sub \
--fd_thres $fd_thres"

# And we're done
echo SUCCESS
