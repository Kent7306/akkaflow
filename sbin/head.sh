#!/bin/bash
#得到文件的绝对目录，$1=当前目录，$2=指定文件
function get_fullpath()
{
    local ddir=`dirname $1`
    cd $ddir
    pwd
}

info=`cat ../config/application.conf | grep -e "http-servers" | grep -Eo "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+"`
if [ -a "$info" ];then
    echo "ERROR!请检查config/application.conf中workflow.node.http-servers的配置"
    exit 1;
fi
arr=(${info//:/ })
host=${arr[0]}
port=8090
mysql_user=`cat ../config/application.conf | grep user | awk 'NR==1' | grep -Eo "\".*?\"" | grep -Eo "[^\"]*?"`
mysql_pwd=`cat ../config/application.conf | grep password | awk 'NR==1' | grep -Eo "\".*?\"" | grep -Eo "[^\"]*?"`
mysql_db=`cat ../config/application.conf | grep jdbc-url | grep mysql | awk 'NR==1' | grep -Eo ":[0-9]+/\w+" | grep -Eo "/\w+" | grep -Eo "\w+"`
mysql_host=`cat ../config/application.conf | grep jdbc-url | grep mysql | awk 'NR==1' | grep -Eo ":/.?*:" | grep -Eo "[^:|^/]+"`
alias akka_mysql="mysql -h$mysql_host -u$mysql_user -p$mysql_pwd"
alias akka_mysql="/home/gzstat/mysql/bin/mysql -h$mysql_host -u$mysql_user --socket /home/gzstat/mysql/mysql.sock -p$mysql_pwd"
#host="127.0.0.1"
#port="8090"

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