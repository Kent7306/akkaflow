#!/bin/bash

info=`cat ../config/application.conf | awk '/[^\/]http-servers\s*=\s*\[/{print $0}' | grep -Eo "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+"`
if [ -a "$info" ];then
    echo "ERROR!请检查config/application.conf中workflow.node.http-servers的配置"
    exit 1;
fi
arr=(${info//:/ })
host=${arr[0]}
port=8090
