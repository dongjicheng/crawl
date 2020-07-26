cd `dirname $0`
source activate jicheng
nohup python jd_price.py >> jd_price.py.log 2>&1 &
