#!/bin/bash
source ~/.bash_profile
echo -e "开始启动Master节点"
./master-startup
sleep 5
echo -e "开始启动Worker节点"
./worker-startup
sleep 1
echo -e "开始启动HttpServer节点"
./httpserver-startup
tail -500f ../logs/run.log &
sleep 15
exit 0
