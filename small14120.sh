#!/bin/bash

#SBATCH -p serial
#SBATCH -n 8 
#SBATCH -t 0-96:00
#SBATCH --mem-per-cpu 3000
#SBATCH -A swahal
#SBATCH -o slurm.%j.out
#SBATCH -e slurm.%j.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=mwymbs@asu.edu
#SBATCH --array=[1-9]

module load python/2.7.9
python small14120.py -work /home/mwymbs/mflog
