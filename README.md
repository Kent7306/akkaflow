## akkaflow
演示系统: [点击这里](http://47.93.186.236:8080/akkaflow-ui/home/login)  用户/密码：admin/admin   
### 简介
`akkaflow`是一个基于`akka`架构上构建的分布式高可用ETL调度工具，可以把一个job中子任务按照拓扑关系在集群中不同的节点上并行执行，高效利用集群资源；提供多个工具节点，可监控文件数据情况，对数据及任务进行监控告警，异常处理等。其中工作流定义相对简洁轻量级，可作为构建数据仓库、或大数据平台上的调度工具。  
整个`akkaflow`架构目前包含有四个节点角色：Master-Active、Master-Standby、Worker、Http-Server，每个角色可以独立部署于不同机器上，支持高可用性（HA），节点中包含以下模块：调度模块，执行模块，告警模块，日志模块，持久化模块。工作流定义文档参考[这里](https://github.com/Kent7306/akkaflow/blob/master/workflow_definition.md)。  
**节点角色关系图**

* `Master` 活动主节点，调度触发工作流实例，分发子任务
* `Master-Standby` 热备份主节点，当主节点宕机，立刻切换为活动主节点
* `Worker` 任务节点，可部署在多个机器上，运行主节点分发过来的任务，并反馈运行结果。
* `Http-Server` http服务节点，提供http API查看操作当前akkaflow系统。  
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E8%8A%82%E7%82%B9%E8%A7%92%E8%89%B2%E5%85%B3%E7%B3%BB%E5%9B%BE.png)  

**Actor对象层级**
	![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/actor%E5%B1%82%E6%AC%A1%E5%85%B3%E7%B3%BB%E5%9B%BE.png)

`akkaflow`工程只是一个后端运行的架构，目前也在不停开发完善中,提供简洁的操作命令；基于B/S的可视化界面已初步开发，提供工作流执行情况等相关信息查看，可视化拖拉生成工作流与调度器的功能尚未开发。  

### 部署
#### 1、打包
* 可以直接在[这里](https://pan.baidu.com/s/1txmQF_dyiitiBsIf5jtS9w)下载`akkaflow-x.x.zip`，，这是已经打包好的程序包
* 可以把工程check out下来，用sbt-native-packager进行编译打包(直接运行`sbt dist`)

#### 2、安装
* 安装环境：Linux系统、jdk1.8或以上、MySQL5.7或以上
* 设置好`JAVA_HOME`环境变量

#### 3、目录说明
* `bin` 存放基本命令（一般不会直接使用）
* `sbin` 存放用户操作命令，如启动节点命令等
* `lib` 存放相关jar包
* `logs` 存放相关日志
* `config` 存放相关配置文件
* `xmlconfig` 存放工作流定义的目录，master节点启动时从该目录载入workflow
* `example` 存放示例相关数据
* `tmp` 存放作为worker执行相关动作节点的临时执行目录，可配置，对应`workflow.action.script-location`

#### 4、安装步骤 (伪分布式部署)：
* 解压到`/your/app/dir`
* mysql数据库准备一个数据库，（如数据库名称为wf，用户密码分别为root）
* 准备一个邮箱，支持smtp方式发送邮件。
* 修改配置文件 `config/application.conf`中以下部分（基本修改数据库以及邮件配置项就可以了）

```scala
  mysql {   //用mysql来持久化数据
  	user = "root"
  	password = "root"
  	jdbc-url = "jdbc:mysql://localhost:3306/wf?useSSL=false"
  	is-enabled = true
  }
  log-mysql {   //把输出日志保持在mysql中
   user = "root"
  	password = "root"
  	jdbc-url = "jdbc:mysql://localhost:3306/wf?useSSL=false"
  	is-enabled = true
  }
  email {	//告警邮箱设置
  	hostname = "smtp.163.com"
  	//smtp端口，可选
  	smtp-port = 465
  	auth = true
  	account = "15018735011@163.com"
  	password = "*****"
  	charset = "utf8"
  	is-enabled = true
  }
}
```
* 其中，因为akkaflow支持分布式部署，当前伪分布部署，可以把master、master-standby、worker、http-servers在同一台机器的不同端口启动，设置jdbc连接，告警邮件设置
* 修改数据库配置文件`conf/db_links.xml`

  ```xml
  <db-links>
	    <link name="local_mysql" type="MYSQL" jdbc-url="jdbc:mysql://localhost:3306/wf" username="root" password="root"/>
	    <link name="local_orcl" type="ORACLE" jdbc-url="xxxxx" username="xxxxx" password="xxxxx"/>
	    <link name="local_hive" type="HIVE" jdbc-url="jdbc:hive://localhost:3306/xxx" username="root" password="root"/>
</db-links>
  ```
  
* 启动角色（独立部署模式）  
  执行: `./standalone-startup`
* 查看启动日志
日志追加到`./logs/run.log`中，tail一下日志，看下启动时是否有异常，无异常则表示启动成功。  
* 查看进程
启动完后，使用jps查看进程  

```
2338 HttpServer
2278 Worker
2124 Master
```

**注意**：akkaflow工作流定义可以存放于xmlconfig下，akkaflow启动时，会自动不断扫描xmlconfig下面的文件，生成对应的worflow提交给Master，所以新建的工作流文件，可以放到该目录中，安装包下的xmlconfig/example下有工作流定义示例。  

### 使用
#### 基于命令行操作
* 角色节点操作命令  
  standalone模式启动：`./standalone-startup.sh`(该模式下会启动master、worker、httpserver)  
 master节点启动：`./master-startup`  
 worker节点启动：`./worker-startup`  
 http_server节点启动：`./httpserver-startup`  
 master-standby节点启动：`bin/master-standby-startup`  
 关闭集群：`./shutdown-cluster`

* akkaflow操作命令集
  `akka`是命令集入口
  ![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E5%91%BD%E4%BB%A4%E9%9B%86%E5%85%A5%E5%8F%A3.jpg) 
  命令集合列表
   ![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E5%91%BD%E4%BB%A4%E9%9B%86%E5%90%88.jpg)
  

**注意:** 除了节点启动命令，把工作流定义的xml文件放在xmlconfig目录下，可自动扫描添加对应工作流或调度器，也可以用命令提交，在akka-ui界面下，工作流与调度器的其他操作可直接操作。  

#### akkaflow-ui可视化界面
akkaflow-ui是分离部署的一套可视化系统，基于访问akkflow数据库与调用接口来展现akkflow的运行信息，与akkflow系统是完全解耦的，并且akkflow-ui暂时未开源。  
* 首页监控页面
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E9%A6%96%E9%A1%B5%E7%9B%91%E6%8E%A7.png)    
* 工作流管理页面
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E5%B7%A5%E4%BD%9C%E6%B5%81%E7%AE%A1%E7%90%86%E9%A1%B5%E9%9D%A2.png)  
* 任务实例告警邮件
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E5%91%8A%E8%AD%A6%E9%82%AE%E4%BB%B6.png) 

### 版本计划
1. 重新封装数据传输节点，关于数据传输节点，本来想集成sqoop1 java api的，但本地生成的java的jdk版本和集群的jdk版本需要一致，考虑到某些集群的jdk版本仍旧是1.6，1.7，而akkaflow的jdk版本起码要1.8或以上，所以还是用sqoop shell（需要节点机器支持sqoop1），并且也没有很好封装sqoop命令；而sqoop2感觉不是很通用简便。
2. 界面集成一个可视化拖拉配置工作流与调度器的开发功能模块（这一块感觉自己做不来），目前的工作流以及调度器主要还是要自己编写xml文件，不够简便。
3. 增加运行节点收集机器性能指标的功能。
4. 外面套一层功能权限管理的模块，区分限制人员角色模块及数据权限，支持多人使用或协助的场景。

* 整套akkaflow框架，包括前后端都是抽空闲时间去做的，个人主要工作方向主要是数据开发与后端开发，所以前端界面并没有做得很漂亮，很多可以展现的东西也没做出来，但是其实很多元数据已经尽量持久化到表中，对于整套框架，基本功能是全的，但是文档方面也没尽责详细说明，越做下去，感觉需要补的东西越来越多，深感一个人的精力有限，所以后面有时间精力再去修复和更新，当然啦，如果其他小伙伴有兴趣的话，欢迎联系加入，无论前后端，毕竟人多力量大。

