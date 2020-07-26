cd `dirname $0`
source activate jicheng
nohup python shoujihao.py >> shoujihao.py 2>&1 &
