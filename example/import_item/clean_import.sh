#!/bin/bash
stime=$1
mysql -uroot -proot -Dwf -e "
create table if not exists example_item (
  ds varchar(128),
  orderid varchar(20),
  goods_name varchar(128)
)
"
rm -f item.result
cat item*.log | while read line
do
    echo "$stime,${line//---/,}" >> item.result
done

mysql -uroot -proot -Dwf -e "delete from example_item where ds = '$stime'"
while read line
do
    str="'"${line//,/\',\'}"'"
    mysql -uroot -proot -Dwf -e "insert into example_item values ($str)"
done <  item.result
