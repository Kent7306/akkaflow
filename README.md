## akkaflow
演示系统: [点击这里](http://47.93.186.236:8080/akkaflow-ui/home/login)  用户/密码：admin/admin
### 简介
`akkaflow`是一个基于`akka`架构上构建的分布式高可用ETL调度工具，可以把一个job中子任务按照拓扑关系在集群中不同的节点上并行执行，高效利用集群资源；提供多个工具节点，可监控文件数据情况，对数据及任务进行监控告警，异常处理等。其中工作流定义类似`Oozie`，相对简洁轻量级，可作为构建数据仓库、或大数据平台上的调度工具。</br>
整个`akkaflow`架构目前包含有四个节点角色：Master-Active、Master-Standby、Worker、Http-Server，每个角色可以独立部署于不同机器上，支持高可用性（HA），节点中包含以下模块：调度模块，执行模块，告警模块，日志模块，持久化模块。工作流定义文档参考[这里](https://github.com/Kent7306/akkaflow/blob/master/workflow_definition.md)，调度器定义文档参考[这里](https://github.com/Kent7306/akkaflow/blob/master/coordinator_definition.md)</br>
**节点角色关系图**</br>
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E8%8A%82%E7%82%B9%E8%A7%92%E8%89%B2%E5%85%B3%E7%B3%BB%E5%9B%BE.png)
</br>

* `Master-Active` 活动主节点，调度触发工作流实例，分发子任务
* `Master-Standby` 热备份主节点，当主节点宕机，立刻切换为活动主节点
* `Worker` 任务节点，可部署在多个机器上，运行主节点分发过来的任务，并反馈运行结果。
* `Http-Server` http服务节点，提供http API查看操作当前akkaflow系统。
**actor对象层级**</br>
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/actor%E5%B1%82%E6%AC%A1%E5%85%B3%E7%B3%BB%E5%9B%BE.png)
</br>
`akkaflow`工程只是一个后端运行的架构，目前也在不停开发完善中；基于B/S的可视化界面已初步开发，提供工作流执行情况等相关信息查看，可视化拖拉生成工作流与调度器的功能尚未开发。</br>
</br>
### 部署
#### 1、打包
* 可以直接在项目中下载`akkaflow-x.x.zip`，这是已经打包好的程序包</br>
* 可以把工程check out下来，用sbt-native-packager进行编译打包</br>
#### 2、安装
* 安装环境：Linux系统、jdk1.8或以上、MySQL5.7或以上</br>
### 3、目录说明
* `bin` 存放基本命令（一般不会直接使用）
* `sbin` 存放用户操作命令，如启动节点命令等
* `lib` 存放相关jar包
* `logs` 存放相关日志
* `config` 存放相关配置文件
* `xmlconfig` 存放工作流与调度器定义的目录，master节点启动时从该目录载入workflow与coordinator
* `example` 存放示例相关数据
* `tmp` 存放作为worker执行相关动作节点的临时执行目录，可配置，对应`workflow.action.script-location`
#### 3、安装步骤：
伪分布式部署
* 解压到`/your/app/dir`</br>
* mysql数据库准备一个数据库，如wf</br>
* 修改配置文件 `config/application.conf`中以下部分</br>
```scala
workflow {
  nodes {   //集群节点
      master = "127.0.0.1:2751"    //主节点，所部署机器的ip与端口
    master_standby = "127.0.0.1:2752"  //备份主节点
  	workers = ["127.0.0.1:2851","127.0.0.1:2852","127.0.0.1:2853"]   //工作节点，所部署机器的ip与端口，支持单个机器上多个工作节点
  	http-servers = ["127.0.0.1:2951"]
  }
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
  	smtp-port = 465
  	account = "15018735011@163.com"
  	password = "ogn88287306"
  	is-enabled = false
  }
  action {	//临时执行脚本的目录
  	script-location = "./tmp"
  }
  xml-loader {	//xml装载器配置
  	workflow-dir = "xmlconfig/workflow"
  	coordinator-dir = "xmlconfig/coordinator"
  	scan-interval = 5   //单位：秒
  }
}
```

其中，因为akkaflow支持分布式部署，当前伪分布部署，可以把master、master-standby、worker、http-servers在同一台机器的不同端口启动，设置jdbc连接，告警邮件设置
* 启动角色（注意顺序）
启动master节点：`bin/master-startup`
启动master-standby节点：`bin/master-standby-startup`
启动worker节点：`bin/worker-startup`
启动http-server服务器：`/bin/httpserver-startup`
* 查看启动日志
日志追加到`./logs/run.log`中，tail一下日志，看下启动时是否有异常，无异常则表示启动成功。
* 查看进程
启动完后，使用jps查看进程
```
2338 HttpServer
2278 Worker
2198 MasterStandbyStartup
2124 MasterStartup
```

**注意**：akkaflow工作流的定义、调度器的定义存放于xmlconfig下，akkaflow启动时，会自动扫描xmlconfig下面的文件，生成对应的worflow或coordinator提交给Master，所以新建的工作流、调度器定义文件，可以放到该目录中，安装包下的xmlconfig/example下有工作流与调度器定义示例。

</br>
### 使用
#### 基于命令行操作
* 节点启动命令
 master节点启动：`./master-startup`
 worker节点启动：`./worker-startup`
 http_server节点启动：`./httpserver-startup`
* 增加工作流 `submit-workflow [file]`
示例： `./submit-workflow /tmp/wf_import_order.xml`
* 增加coordinator`submit-coor [file]`
示例： `./submit-coor /tmp/coor_import_order.xml`
* 删除工作流`del-workflow [wf_name]`
示例： `del-workflow wf_import_order`
* 删除coordintor `del-coor [coor_name]`
示例：`./del-workflow coor_import_order`
* 杀死工作流实例`kill-wf-instance [instance_id]`
示例: `./kill-wf-instance ac733154`
* 重跑工作流实例`rerun-wf-instance [instance_id]`
示例：`./rerun-wf-instance ac733154`
**注意: **除了节点启动命令，其他的命令日常中不会经常直接使用，因为把工作流或调度器定义的xml文件放在xmlconfig目录下，可自动扫描添加对应工作流或调度器，而工作流与调度器的其他操作可在akkflow-ui上直接操作。

#### akkaflow-ui可视化界面
akkaflow-ui是独立部署的一套可视化系统，基于访问akkflow数据库与调用接口来展现akkflow的运行信息，与akkflow系统是完全解耦的，并且akkflow-ui暂时未开源。</br>
* 首页监控页面</br>
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E9%A6%96%E9%A1%B5%E7%9B%91%E6%8E%A7.png)
* 工作流实例页面</br>
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E5%B7%A5%E4%BD%9C%E6%B5%81%E5%AE%9E%E4%BE%8B%E9%A1%B5%E9%9D%A2.png)
* coordinator管理页面</br>
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E8%B0%83%E5%BA%A6%E5%99%A8%E7%AE%A1%E7%90%86%E9%A1%B5%E9%9D%A2.png)
* 工作流管理页面</br>
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E5%B7%A5%E4%BD%9C%E6%B5%81%E7%AE%A1%E7%90%86%E9%A1%B5%E9%9D%A2.png)
* 工作流实例列表页面</br>
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E5%B7%A5%E4%BD%9C%E6%B5%81%E6%9F%A5%E8%AF%A2%E5%88%97%E8%A1%A8.png)
</br>


