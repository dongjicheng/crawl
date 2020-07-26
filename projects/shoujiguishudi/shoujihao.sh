cd `dirname $0`
source activate jicheng
nohup python shoujihao.py >> shoujihao.py.log 2>&1 &
