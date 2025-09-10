#!/bin/sh

#SBATCH -J 250724_us_deid_s03
#SBATCH -c 12
#SBATCH -n 1
#SBATCH --mem 100000
#SBATCH --output=/home/hellern/isilon/code/ccf/DICOM-DeID/upl_out.txt
#SBATCH --error=/home/hellern/isilon/code/ccf/DICOM-DeID/upl_err.txt

#SBATCH -p xtreme


python3 /home/hellern/isilon/code/ccf/DICOM-DeID/upload_s3.py \
    /home/hellern/isilon/data/weaver_projects/request_176_deid_v2_zipped_tarred \
    s3://myawsmisc/ccf_us_deid/


