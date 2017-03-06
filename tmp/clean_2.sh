#!/bin/bash

sdate='2017-03-04'
cd "/Users/kent/tmp/akka_test/log/$sdate"
rm -f item.result
cat item*.out|while read line
do
    echo "$sdate,${line//---/,}" >> item.result
done
