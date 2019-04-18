## akkaflow  
### 简介
`akkaflow`是一个基于`akka`架构上构建的分布式高可用ETL工作流调度工具，可以把任务分发在集群中不同的节点上并行执行，高效利用集群资源，支持时间及任务混合触发；提供多种节点类型。其中工作流由xml文件，并且提供一套完整的基于Shell的操作命令集，简洁易用，长期稳定运行，可作为构建数据仓库、或大数据平台上的调度工具。  
用户提交的xml工作流定义文件，满足触发条件后，系统会触发执行工作流；实例运行产生的各类数据将被记录并提供用户查看与进一步操作，其中


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

### 部署
#### 1、打包
* 可以直接在[这里](https://pan.baidu.com/s/1K7CeCLW8D6qW62r5qbCYug)下载`akkaflow-x.x.zip`，这是已经打包好的程序包
* 可以把工程拉下来，用sbt-native-packager进行编译打包(直接运行`sbt dist`)

#### 2、安装
* 安装环境：Linux系统（UTF8编码环境）或MacOS、jdk1.8或以上、MySQL5.7或以上(UTF8编码)

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
  workflow {
  node {
    master {    		//主节点，所部署机器端口，目前只支持单主节点
      hostname = "127.0.0.1"  	//若内网，则为内网IP，若公网部署，则为公网IP，整个集群中，master ip是固定的
      port = 2751
      standby {  	//备份主节点
        port = 2752
      }
    }
    worker {  		//工作节点，所部署机器端口，支持单个机器上多个工作节点 
      ports = [2851, 2852, 2853]
    }
    http-server{		//http-server节点
      port = 2951
      connector-port = 8090  //http访问端口
    }
  }
  current.inner.hostname = "127.0.0.1"  //当前机器内网IP
  current.public.hostname = "127.0.0.1"  //当前机器公网IP，若部署在内网，则与内网IP一致
  
  mysql {   //用mysql来持久化数据
  	user = "root"
  	password = "root"
  	jdbc-url = "jdbc:mysql://localhost:3306/wf?useSSL=false&autoReconnect=true&failOverReadOnly=false"
  	is-enabled = true
  }
  email {	//告警邮箱设置
  	hostname = "smtp.163.com"
  	smtp-port = 25 	  //smtp端口，可选
  	auth = true
  	account = "15018735011@163.com"
  	password = "******"   //这里改成自己的邮箱密码哈
  	charset = "utf8"
  	is-enabled = true
  	node-retry-fail-times = 1	//节点执行n次失败才发出重试失败告警
  }
  extra {
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

**注意**：akkaflow工作流定义可以存放于xmlconfig下，akkaflow启动时，会自动并一直扫描xmlconfig下面的文件，生成对应的worflow，目录xmlconfig/example下有工作流定义示例。

#### 5、关闭集群  
执行`./sbin/stop-cluster`, 关闭集群系统

### 命令使用
#### 1、角色节点操作命令  
  * standalone模式启动：`sbin/standalone-startup`(该模式下会启动master、worker、http-server)  
 * master节点启动：`sbin/master-startup`  
 * worker节点启动：`sbin/worker-startup`  
 * http-server节点启动：`sbin/httpserver-startup`  
 * master-standby节点启动：`sbin/master-standby-startup`  
 * 关闭集群：`sbin/stop-cluster`

### 版本计划
1. 界面集成一个可视化拖拉配置工作流与调度器的开发功能模块（这一块感觉自己做不来,有兴趣的前端开发同学可以联系我，共同合作开发），目前的工作流以及调度器主要还是要自己编写xml文件，不够简便。
2. 增加运行节点收集机器性能指标的功能。
3. 外面套一层功能权限管理的模块，区分限制人员角色模块及数据权限，支持多人使用或协助的场景。

