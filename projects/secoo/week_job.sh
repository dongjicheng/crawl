cd `dirname $0`
source /home/u9000/anaconda3/bin/activate jicheng
nohup python week_job.py >> week_job.py.log 2>&1 &
