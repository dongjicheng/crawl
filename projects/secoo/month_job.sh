cd `dirname $0`
source /home/u9000/anaconda3/bin/activate jicheng
nohup python month_job.py >> month_job.py.log 2>&1 &
