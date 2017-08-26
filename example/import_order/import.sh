#!/bin/bash

sdate="2017-03-04"
mysql -uroot -proot -Dtest -e "delete from t_order where ds = '$sdate'"
while read line
do
    str="'"${line//,/\',\'}"'"
    mysql -uroot -proot -Dtest -e "insert into t_order values ($str)"
done <  /Users/kent/tmp/akka_test/log/$sdate/order.result

