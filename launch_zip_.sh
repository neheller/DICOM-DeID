#!/bin/sh

#SBATCH -J 250724_us_deid_s02
#SBATCH -c 12
#SBATCH -n 1
#SBATCH --mem 100000
#SBATCH --output=/home/hellern/isilon/code/ccf/DICOM-DeID/zip_out.txt
#SBATCH --error=/home/hellern/isilon/code/ccf/DICOM-DeID/zip_err.txt

#SBATCH -p xtreme


python3 /home/hellern/isilon/code/ccf/DICOM-DeID/batch_zip.py \
    /home/hellern/isilon/data/weaver_projects/request_176_deid_v2 \
    /home/hellern/isilon/data/weaver_projects/request_176_deid_v2_zipped


