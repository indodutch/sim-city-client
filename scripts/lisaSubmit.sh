# shell for the job:
#PBS -S /bin/bash
# job requires at most 24 hours
#     and 0 seconds wallclock time and uses one 12-core node:
#PBS -lwalltime=24:00:00 -lnodes=1:cores12
# cd to the directory where the program is to be called:
# call the program

module load python/2.7.3
. ~/simcity/bin/activate
cd $PBS_O_WORKDIR
python runExample.py -D 1
