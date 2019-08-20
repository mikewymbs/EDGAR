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
#SBATCH --array=[0-99]

module load python/2.7.9
python all_ip_org.py -work /home/mwymbs/Downloads/IPs/Locations 

