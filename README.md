## akkaflow  
### 简介
`akkaflow`是一个基于`akka`架构上构建的分布式高可用ETL调度工具，可以把任务分发在集群中不同的节点上并行执行，高效利用集群资源，支持时间及任务混合触发；提供多种节点类型。其中工作流由xml文件，并且提供一套完整的基于Shell的操作命令集，简洁易用，可作为构建数据仓库、或大数据平台上的调度工具。  
用户基于命令集向系统提交工作流定义的xml文件，满足触发条件后，系统会触发执行工作流，用户可通过命令集查看任务执行情况。其中

* 工作流定义文档参考[这里](https://github.com/Kent7306/akkaflow/blob/master/workflow_definition.md)
* 操作使用示例参考[这里](https://github.com/Kent7306/akkaflow/blob/master/usage.md)
* 命令集操作文档详见下面使用章节

整个`akkaflow`架构目前包含有四个节点角色：Master、Master-Standby、Worker、Http-Server，每个角色可以独立部署于不同机器上，支持高可用。

**节点角色关系图**

* `Master` 活动主节点，调度触发工作流实例，分发子任务
* `Master-Standby` 热备份主节点，当主节点宕机，立刻切换为活动主节点
* `Worker` 任务节点，可部署在多个机器上，运行主节点分发过来的任务，并反馈运行结果。
* `Http-Server` http服务节点，提供http API查看操作当前akkaflow系统。  
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E8%8A%82%E7%82%B9%E8%A7%92%E8%89%B2%E5%85%B3%E7%B3%BB%E5%9B%BE.png)    

### 部署
#### 1、打包
* 可以直接在[这里](https://pan.baidu.com/s/1txmQF_dyiitiBsIf5jtS9w)下载`akkaflow-x.x.zip`，，这是已经打包好的程序包
* 可以把工程check out下来，用sbt-native-packager进行编译打包(直接运行`sbt dist`)

#### 2、安装
* 安装环境：Linux系统（UTF8编码环境）、jdk1.8或以上、MySQL5.7或以上
* 设置好`JAVA_HOME`环境变量

#### 3、目录说明
* `sbin` 存放用户操作命令，如启动节点命令等
* `lib` 存放相关jar包
* `logs` 存放相关日志
* `config` 存放相关配置文件
* `xmlconfig` 存放示例工作流文件，系统启动时会自动载入

#### 4、安装步骤 (伪分布式部署)：
* 解压到`/your/app/dir`
* 准备一个mysql数据库，（如数据库名称为wf，用户密码分别为root/root）
* 准备一个邮箱账户（推荐用网易邮箱），支持smtp方式发送邮件。
* 修改配置文件 `config/application.conf`中以下部分（基本修改数据库以及邮件配置项就可以了）

```scala
  mysql {   //用mysql来持久化数据
  	user = "root"
  	password = "root"
  	jdbc-url = "jdbc:mysql://localhost:3306/wf?autoReconnect=true"
  	is-enabled = true
  }
  email {	//告警邮箱设置
  	hostname = "smtp.163.com"
  	smtp-port = 465  //smtp端口，可选
  	auth = true
  	account = "15018735011@163.com"
  	password = "******"
  	charset = "utf8"
  	is-enabled = true
  }
  extra {  //hdfs集群配置
  	hdfs-uri = "hdfs://quickstart.cloudera:8020"
  }
```
  
* 启动角色（独立部署模式）  
其中，当前伪分布部署，是把master、worker、http-servers在同一台机器的不同端口启动  
  执行: `./standalone-startup`
* 查看启动日志  
启动时会在终端打印日志，另外，日志也追加到`./logs/run.log`中，tail一下日志，看下启动时是否有异常，无异常则表示启动成功。  
* 查看进程  
启动完后，使用jps查看进程  

```
2338 HttpServer
2278 Worker
2124 Master
```

**注意**：akkaflow工作流定义可以存放于xmlconfig下，akkaflow启动时，会自动不断扫描xmlconfig下面的文件，生成对应的worflow提交给Master，所以工作流文件，也可以放到该目录中，安装包下的xmlconfig/example下有工作流定义示例。

#### 5、关闭集群  
执行`./sbin/shutdown-cluster`, 关闭集群系统

### 命令使用
#### 1、角色节点操作命令  
  * standalone模式启动：`sbin/standalone-startup.sh`(该模式下会启动master、worker、httpserver)  
 * master节点启动：`sbin/master-startup`  
 * worker节点启动：`sbin/worker-startup`  
 * http_server节点启动：`sbin/httpserver-startup`  
 * master-standby节点启动：`sbin/master-standby-startup`  
 * 关闭集群：`sbin/shutdown-cluster`

#### 2、akkaflow操作命令集
##### 命令集入口
  ```shell
  kentdeMacBook-Pro:sbin kent$ ./akka
     _     _     _            __  _
    / \   | | __| | __ __ _  / _|| |  ___ __      __
   / _ \  | |/ /| |/ // _` || |_ | | / _ \\ \ /\ / /
  / ___ \ |   < |   <| (_| ||  _|| || (_) |\ V  V /
 /_/   \_\|_|\_\|_|\_\\__,_||_|  |_| \___/  \_/\_/

【使用】
	akka [ front| instance| workflow| util]
【说明】
	akkaflow调度系统命令入口。
	1、akka front 调度系统首页
	2、akka instance 工作流实例操作命令集，详见该命令
	3、akka workflow 工作流操作命令集，详见该命令
	4、akka util 辅助操作命令集，详见该命令
【示例】
	akka front
	akka instance -info -log 574de284 (查看某实例)
	akka workflow -kill 574de284 (杀死某运行中的实例) 
  ```
	
##### 命令集合列表
   ![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E5%91%BD%E4%BB%A4%E9%9B%86%E5%90%88.jpg)
  

**注意:** 除了节点启动命令，把工作流定义的xml文件放在xmlconfig目录下，可自动扫描添加对应工作流或调度器，也可以用命令提交. 

#### 任务实例告警邮件
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E5%91%8A%E8%AD%A6%E9%82%AE%E4%BB%B6.png) 

### 版本计划
1. 界面集成一个可视化拖拉配置工作流与调度器的开发功能模块（这一块感觉自己做不来），目前的工作流以及调度器主要还是要自己编写xml文件，不够简便。
2. 增加运行节点收集机器性能指标的功能。
3. 外面套一层功能权限管理的模块，区分限制人员角色模块及数据权限，支持多人使用或协助的场景。

