#!/bin/bash
cd `dirname $0`
source ./head.sh
./master-startup
sleep 5
./httpserver-startup
sleep 3
./worker-startup
sleep 2
tail -1000f ../logs/run.log &
sleep 10
ps -ef | grep tail | grep "/logs/run.log" | awk '{print $2}' | xargs kill -9
exit 0