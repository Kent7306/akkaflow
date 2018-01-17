#!/bin/bash
cd `dirname $0`
source ./head.sh

for f in `ls *`;do
    iconv -f utf8 -t $local_lang $f -o $f
done

f=../config/application.conf
iconv -f utf8 -t $local_lang $f -o $f
f=../config/create_table.sql 
iconv -f utf8 -t $local_lang $f -o $f
