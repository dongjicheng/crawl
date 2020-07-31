cd `dirname $0`
source /home/u9000/anaconda3/bin/activate jicheng
nohup python shoujihao.py >> shoujihao.py.log 2>&1 &
