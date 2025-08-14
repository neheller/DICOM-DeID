#!/bin/sh

#SBATCH -J 250724_us_deid_s02
#SBATCH -c 12
#SBATCH -n 1
#SBATCH --mem 100000
#SBATCH --output=/home/hellern/isilon/code/ccf/DICOM-DeID/gpu_out.txt
#SBATCH --error=/home/hellern/isilon/code/ccf/DICOM-DeID/gpu_err.txt

#SBATCH -p gpu
#SBATCH --gres=gpu:1


python3 /home/hellern/isilon/code/ccf/DICOM-DeID/local_deid.py

