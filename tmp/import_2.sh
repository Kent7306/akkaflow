#!/bin/bash

sdate="$1"
/usr/local/mysql/bin/mysql -uroot -proot -Dtest -e "delete from t_item where ds = '$sdate'"
while read line
do
    str="'"${line//,/\',\'}"'"
    /usr/local/mysql/bin/mysql -uroot -proot -Dtest -e "insert into t_item values ($str)"
done <  /tmp/log_test/$sdate/item.result

