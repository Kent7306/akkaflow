## akkaflow  
### 简介
`akkaflow`是一个基于`akka`架构上构建的分布式高可用ETL工作流调度工具，可以把任务分发在集群中不同的节点上并行执行，高效利用集群资源，支持时间周期及任务动态调度，提供多种运行节点。目前工作流由xml文件定义，并且提供一套完整的基于Shell的操作命令集，相对简洁，可作为构建数据仓库、或数据平台上的调度工具。  
用户提交工作流，条件满足后触发生成实例，系统运行产生的元数据将被记录并提供用户查看与进一步操作，其中

* 简单的前端操作页面详见[演示地址](http://148.70.11.221:8080/login)，演示账号密码分别为admin/123，配置为（3台公网机器，1内核，1G内存）
* 工作流定义文档详见[这里](https://github.com/Kent7306/akkaflow/blob/master/workflow_definition.md)   
* 使用示例文档点击[这里](https://github.com/Kent7306/akkaflow/blob/master/usage.md)

整个`akkaflow`架构目前包含有四个节点角色：Master、Master-Standby、Worker、Http-Server，每个角色可以独立部署于不同机器上，支持高可用。

**节点角色关系图**

* `Master` 活动主节点，调度触发工作流实例，分发子任务
* `Master-Standby` 热备份主节点，当主节点宕机，立刻切换为活动主节点
* `Worker` 任务节点，可部署在多个机器上，运行节点任务
* `Http-Server` http服务节点，接受请求
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E8%8A%82%E7%82%B9%E8%A7%92%E8%89%B2%E5%85%B3%E7%B3%BB%E5%9B%BE.png)    

### 安装配置
#### 1、从docker开始
目前已制作一份docker服务编排配置文件，通过docker直接快速初始化系统环境，体验系统的使用。
从百度网盘下载 https://pan.baidu.com/s/1erZbngufjK5dw6vPYJuAaQ
```shell
#解压
tar -xvf docker-akkalfow-2.12.1.tar.gz
#进入目录
cd docker
#构建镜像
docker-compose build
# 执行启动系统
docker-compose up
```
等系统启动完毕后，过程大概30s，打开浏览器，输入：http://localhost:8080/login, 管理员用户密码分别为: admin/123，点击“登录”，enjoin it！


#### 2、打包
* 也可以直接在[这里](https://pan.baidu.com/s/1RAiyP-0p1l4qOIRY-mbIWA)下载`akkaflow-x.x.zip`，这是已编译的安装包，只包括后端调度系统。
* 或把代码拉下来，用sbt-native-packager进行编译打包(目录下直接运行`sbt dist`)

#### 2、安装
* 安装环境：Linux系统（UTF8编码环境）或MacOS、jdk1.8或以上、MySQL5.7或以上(UTF8编码)

#### 3、目录说明
* `sbin` 存放用户操作命令，如启动节点命令等
* `lib` 存放相关jar包
* `logs` 存放相关日志
* `config` 存放相关配置文件
* `xmlconfig` 存放示例工作流文件，系统启动时会自动载入

#### 4、安装步骤 (单点部署)：
* 解压到`/your/app/dir`
* 准备一个mysql数据库，（如数据库名称为wf，用户密码分别为root/root）
* 准备一个邮箱账户（推荐用网易邮箱），支持smtp方式发送邮件。
* 修改配置文件 `config/application.conf`中以下部分（基本修改数据库以及邮件配置项就可以了）

```scala
  //配置
workflow {
  node {
    master {        //主节点，所部署机器端口，目前只支持单主节点
      hostname = "127.0.0.1"    //若内网，则为内网IP，若公网部署，则为公网IP，一个集群中，这个是固定的
      port = 2751
    }
    standby { //备份主节点
      port = 2752
    }
    worker {      //工作节点，所部署机器端口，支持单个机器上多个工作节点
      ports = [2851, 2852, 2853]
    }
    http-server {    //http-server节点
      port = 2951
      http-port = 8090  //http访问端口
    }
  }
  current.inner.hostname = "127.0.0.1"      //当前机器内网IP
  current.public.hostname = "127.0.0.1"          //当前机器公网IP，若部署在内网，则与内网IP一致

  mysql {   //用mysql来持久化数据
    user = "root"
    password = "root"
    jdbc-url = "jdbc:mysql://localhost:3306/wf?useSSL=false&autoReconnect=true&failOverReadOnly=false"
    max-active = 10
    init-size = 5
    min-idle = 5
    max-wait = 6000
    validation-query = "select 1"
    min-evictable-idle-time-millis = 300000

  }
  email {  //告警邮箱设置
    hostname = "smtp.126.com"
    smtp-port = 25    //smtp端口，可选
    auth = true
    account = "akkaflow@126.com"
    nickname = "测试任务告警"
    password = "******"
    charset = "utf8"
    is-enabled = true
    node-retry-fail-times = 1  //节点执行n次失败才发出重试失败告警
  }
  xml-loader {  //xml装载器配置
    workflow-dir = "xmlconfig"
    scan-interval = 5   //单位：秒
  }
  cron-runner { //定时器配置
    reset = "59 23 * * *"   //定期重置所有工作流状态
    plan = "1 0 * * *"
  }
}

```
  
* 启动角色（独立部署模式）  
其中，单节点部署，是把master、worker、http-servers在同一台机器以不同进程启动 
  执行: `./standalone-startup`
* 查看启动日志  
启动时会在终端打印日志，另外，日志也追加到`./logs/run.log`中，tail一下日志，看下启动时是否有异常，无异常则表示启动成功。  
* 查看进程  
启动后，使用jps查看进程  

```
2338 HttpServer
2278 Worker
2124 Master
```

**注意**：akkaflow工作流定义文件可以存放于目录xmlconfig下，akkaflow启动时，会自动并一直扫描xmlconfig下面的文件，生成对应的worflow，目录xmlconfig/example下有工作流定义示例。

#### 5、关闭集群  
执行`./sbin/stop-cluster`, 关闭集群系统

### 命令使用
#### 1、操作命令
* 统一操作命令入口：`sbin/akka`
#### 2、角色节点操作命令  
 * standalone模式启动：`sbin/standalone-startup`(该模式下单机会启动master、worker、master-standby、http-server)  
 * master节点启动：`sbin/master-startup`  
 * worker节点启动：`sbin/worker-startup`  
 * http-server节点启动：`sbin/httpserver-startup`  
 * master-standby节点启动：`sbin/master-standby-startup`  
 * 关闭集群：`sbin/stop-cluster`

### 版本计划
1. 界面集成一个可视化拖拉配置工作流与调度器的开发功能模块（这一块感觉自己做不来,有兴趣的前端开发同学可以联系我，共同合作开发）。
2. 增加运行节点收集机器性能指标的功能。
3. 外面套一层功能权限管理的模块，区分限制人员角色模块及数据权限，支持多人使用或协助的场景。

