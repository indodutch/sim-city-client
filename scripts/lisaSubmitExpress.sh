# shell for the job:
#PBS -S /bin/bash
# job requires at most 0 hours, 5 minutes
#     and 0 seconds wallclock time and uses one 12-core node:
#PBS -lwalltime=00:05:00 -lnodes=1
# cd to the directory where the program is to be called:
# call the program

module load python/2.7.9
. ~/simcity/bin/activate
cd $PBS_O_WORKDIR
simcity run -M 5
