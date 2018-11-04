#!/bin/bash
#得到文件的绝对目录，$1=当前目录，$2=指定文件
function get_fullpath()
{
    local ddir=`dirname $1`
    cd $ddir
    pwd
}

#日期计算函数，屏蔽底层linux与mac的date命令差异
function getdate()
{
    stadate=$1
    invetal_day=$2
    is_linux=`uname | grep -i "Linux" | wc -l`
    if [ $is_linux -eq 1 ];then
        echo `date +"%F" -d "${stadate} ${invetal_day} day"`
    else
        echo `date -v${invetal_day}d -j -f %Y-%m-%d ${stadate} +"%F"`
    fi
}

info=`cat ../config/application.conf | grep -e "http-servers" | grep -Eo "[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+"`
if [ -a "$info" ];then
    echo "ERROR!请检查config/application.conf中workflow.node.http-servers的配置"
    exit 1;
fi
arr=(${info//:/ })
host=${arr[0]}
port=8090
mysql_user=`cat ../config/application.conf | grep user | awk 'NR==1' | grep -Eo "\".*?\"" | grep -Eo "[^\"]+"`
mysql_pwd=`cat ../config/application.conf | grep password | awk 'NR==1' | grep -Eo "\".*?\"" | grep -Eo "[^\"]+"`
mysql_db=`cat ../config/application.conf | grep jdbc-url | grep mysql | awk 'NR==1' | grep -Eo ":[0-9]+/\w+" | grep -Eo "/\w+" | grep -Eo "\w+"`
mysql_host=`cat ../config/application.conf | grep jdbc-url | grep mysql | awk 'NR==1' | grep -Eo ":/.*?:" | grep -Eo "[^:|^/]+"`
alias akka_mysql="mysql -h$mysql_host -u$mysql_user -p$mysql_pwd"
#alias akka_mysql="/home/gzstat/mysql/bin/mysql -h$mysql_host -u$mysql_user --socket /home/gzstat/mysql/mysql.sock -p$mysql_pwd"

local_lang=`echo ${LANG##*.}`
#echo $local_lang

red='\033[0;41m' # 红色
RED='\033[1;31m'
green='\033[0;32m' # 绿色
GREEN='\033[1;32m'
yellow='\033[5;43m' # 黄色
YELLOW='\033[1;33m'
blue='\033[0;34m' # 蓝色
BLUE='\033[1;34m'
purple='\033[0;35m' # 紫色
PURPLE='\033[1;35m'
cyan='\033[4;36m' # 蓝绿色
CYAN='\033[1;36m'
WHITE='\033[1;37m' # 白色

NC='\033[0m' # 没有颜色
