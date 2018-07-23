#!/bin/bash
cd `dirname $0`
source ./head.sh
sh ./master-startup
sleep 5
sh ./httpserver-startup
sleep 3
sh ./worker-startup
sleep 2
tail -1000f ../logs/run.log &
sleep 10
ps -ef | grep tail | grep "tail -1000" | awk '{print $2}' | xargs kill -9 1>/dev/null 2>&1
exit 0