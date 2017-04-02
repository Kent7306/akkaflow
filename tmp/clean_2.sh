#!/bin/bash

sdate=$1
cd "/tmp/log_test/$sdate"
rm -f item.result
cat item*.out|while read line
do
    echo "$sdate,${line//---/,}" >> item.result
done
