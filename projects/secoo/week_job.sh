cd `dirname $0`
source activate jicheng
nohup python week_job.py >> week_job.py.log 2>&1 &
