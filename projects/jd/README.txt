set run_date=脚本运行日期
jd_price.sh  输入 resource/month202006(format:商品id)  输出  mongo:jingdong/jdprice+${run_date}(format:商品id p1 p2 p3 ...)
GetBookPub.sh 输入 resource/newCateName(format:分类1-分类2-分类3)  输出  mongo:jingdong/brand+${run_date(format:brand_id,name)