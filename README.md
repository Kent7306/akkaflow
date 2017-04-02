## akkaflow</br>
### 简介</br>
`akkaflow`是一个基于`akka`架构上构建的分布式ETL调度工具，可以把任务拆分在集群中不同的节点上运行，高效利用集群资源，可监控文件数据情况，对数据及任务进行监控告警，异常处理。其中工作流定义参考`Oozie`，相对简洁轻量级，可作为构建数据仓库、或大数据平台上的调度工具。</br>
</br>
整个`akkaflow`架构目前包含有三个节点角色：Master、Worker、Http-Server，每个角色可以独立部署于不同机器上，高可用性`Hight Available(HA)`在未来的开发计划当中，节点中包含以下模块：调度模块，执行模块，告警模块，日志模块，持久化模块。工作流定义文档参考[这里](https://github.com/Kent7306/akkaflow/blob/master/workflow_definition.md)，调度器定义文档参考[这里](https://github.com/Kent7306/akkaflow/blob/master/coordinator_definition.md)</br>
</br>
`akkaflow`工程只是一个后端运行的架构，目前也在不停开发完善中，基于浏览器的可视化界面后续会开发，提供工作流实例的执行情况查看，基于界面的工作流调度器拖拉配置生成，分组管理各类信息。</br>
</br>
### 部署
后续补上</br>
</br>
### 使用
#### 基于命令行操作
* 节点启动命令</br>
 master节点启动：`./master-startup`</br>
 worker节点启动：`./worker-startup`</br>
 http_server节点启动：`./httpserver-startup`</br>
* 增加工作流 `submit-workflow [file]`</br>
示例： `./submit-workflow /tmp/wf_import_order.xml`</br>
* 增加coordinator`submit-coor [file]`</br>
示例： `./submit-coor /tmp/coor_import_order.xml`</br>
* 删除工作流`del-workflow [wf_name]`</br>
示例： `del-workflow wf_import_order`</br>
* 删除coordintor `del-coor [coor_name]`</br>
示例：`./del-workflow coor_import_order`</br>
* 杀死工作流实例`kill-wf-instance [instance_id]`</br>
./kill-wf-instance ac733154<br>
* 重跑工作流实例`rerun-wf-instance [instance_id]`</br>
示例：`./rerun-wf-instance ac733154`</br>

#### akkaflow-ui可视化界面</br>
akkaflow-ui是独立部署的一套可视化系统，基于访问akkflow数据库与调用接口来展现akkflow的运行信息，与akkflow系统是完全解耦的，并且akkflow-ui暂时未开源。</br>
* 首页监控页面</br>
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E9%A6%96%E9%A1%B5%E7%9B%91%E6%8E%A7.png)
* 工作流实例页面</br>
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E5%B7%A5%E4%BD%9C%E6%B5%81%E5%AE%9E%E4%BE%8B%E9%A1%B5%E9%9D%A2.png)
* coordinator管理页面</br>
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E8%B0%83%E5%BA%A6%E5%99%A8%E7%AE%A1%E7%90%86%E9%A1%B5%E9%9D%A2.png)
* 工作流管理页面</br>
![Aaron Swartz](https://raw.githubusercontent.com/Kent7306/akkaflow/master/resources/img/%E5%B7%A5%E4%BD%9C%E6%B5%81%E7%AE%A1%E7%90%86%E9%A1%B5%E9%9D%A2.png)
</br>


### Short Introduce
This is a distributed ETL dispatching system based on akka, it has three roles(http-server, master, worker) which can be deployed alone on various machines. But high availability has not yet been developed </br>
</br>
So far several modules are contained as follows: coordinator module, dispatch module, warning module, log module, persistence module. In some case it likes oozie, but more lightweight and easier. You can configure workflows and coordinator with xml file, eg. `workflow_definition.md` and `coordinator_definition.md`.</br>
</br>
This project is just a back-end system, and is still under coding frequently, the document of how to deploy will be provided later. The GUI based on browser will be provided later soon (it has not been developed yet).</br>
</br>


