cd `dirname $0`
source activate jicheng
nohup python month_job.py >> month_job.py.log 2>&1 &
