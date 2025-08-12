#!/bin/sh

#SBATCH -J 250724_us_deid_s02
#SBATCH -c 12
#SBATCH -n 1
#SBATCH --mem 100000
#SBATCH --output=/home/hellern/isilon/code/ccf/DICOM-DeID/out.txt
#SBATCH --error=/home/hellern/isilon/code/ccf/DICOM-DeID/err.txt

#SBATCH -p xtreme


python3 /home/hellern/isilon/code/ccf/DICOM-DeID/local_deid.py

