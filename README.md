##akkaflow
###简介
`akkaflow`是一个基于`akka`架构上构建的分布式ETL调度工具，可以把任务拆分在集群中不同的节点上运行，高效利用集群资源，可监控文件数据情况，对数据及任务进行监控告警，异常处理。其中工作流定义参考`Oozie`，相对简洁轻量级，可作为构建数据仓库、或大数据平台上的调度工具。</br>
整个`akkaflow`架构目前包含有三个节点角色：Master、Worker、Http-Server，每个角色可以独立部署于不同机器上，高可用性`Hight Available(HA)`在未来的开发计划当中，节点中包含以下模块：调度模块，执行模块，告警模块，日志模块，持久化模块。工作流定义文档参考[这里](https://github.com/Kent7306/akkaflow/blob/master/workflow_definition.md)，调度器定义文档参考[这里](https://github.com/Kent7306/akkaflow/blob/master/coordinator_definition.md)</br>
`akkaflow`工程只是一个后端运行的架构，目前也在不停开发完善中，基于浏览器的可视化界面后续会开发，提供工作流实例的执行情况查看，基于界面的工作流调度器拖拉配置生成，分组管理各类信息。</br>
###部署
后续补上
###使用
后续补上</br>
</br>


### Short Introduce
This is a distributed ETL dispatching system based on akka, it has three roles(http-server, master, worker) which can be deployed alone on various machines. But high availability has not yet been developed </br>
So far several modules are contained as follows: coordinator module, dispatch module, warning module, log module, persistence module. In some case it likes oozie, but more lightweight and easier. You can configure workflows and coordinator with xml file, eg. `workflow_definition.md` and `coordinator_definition.md`.</br>
This project is just a back-end system, and is still under coding frequently, the document of how to deploy will be provided later. The GUI based on browser will be provided later soon (it has not been developed yet).</br>


