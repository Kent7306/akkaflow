#!/bin/bash
source ~/.bash_profile

info=`cat ../config/application.conf | grep -e "http-servers" | grep -Eo "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+"`
if [ -a "$info" ];then
    echo "ERROR!请检查config/application.conf中workflow.node.http-servers的配置"
    exit 1;
fi
arr=(${info//:/ })
host=${arr[0]}
port=8090
local_lang=`echo ${LANG##*.}`

red='\e[0;41m' # 红色
RED='\e[1;31m'
green='\e[0;32m' # 绿色
GREEN='\e[1;32m'
yellow='\e[5;43m' # 黄色
YELLOW='\e[1;33m'
blue='\e[0;34m' # 蓝色
BLUE='\e[1;34m'
purple='\e[0;35m' # 紫色
PURPLE='\e[1;35m'
cyan='\e[4;36m' # 蓝绿色
CYAN='\e[1;36m'
WHITE='\e[1;37m' # 白色

NC='\e[0m' # 没有颜色
