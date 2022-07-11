#!/bin/bash

# Fail whenever something is fishy; use -x to get verbose logfiles
set -e -u -x

# Parse arguments from the job scheduler as variables
bids_dir=$1
container=$2
fd_thres=$3

# Enable use of Singularity containers
module load singularity

# Go into the BIDS dataset
cd "$bids_dir"

# Participant level quality control
datalad containers-run \
  --container-name "$container" \
  --input . \
  --output code/qc/ \
  --message "Create group level quality reports" \
  --explicit "\
$bids_dir {outputs} group \
--nprocs $SLURM_CPUS_PER_TASK \
--mem $((SLURM_MEM_PER_NODE / 1024)) \
--float32 \
--work-dir /tmp/ \
--verbose-reports \
--no-sub \
--fd_thres $fd_thres"

# And we're done
echo SUCCESS
