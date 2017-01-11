## Short Introduce
This is a distributed ETL dispatching system based on akka, it has three roles(http-server, master, worker) which can be deployed alone on various machines. But high availability has not yet been developed </br>
</br>
So far several modules are contained as follows: coordinator module, dispatch module, warning module, log module, persistence module. In some case it likes oozie, but more lightweight and easier. You can configure workflows and coordinator with xml file, eg. `wf.xml` and `coordinate.xml`.</br>
</br>
This project is just a back-end system, and is still under coding frequently, the document of how to deploy will be provided later. The GUI based on browser will be provided later soon (it has not been developed yet).</br>


##akkaflow
`akkaflow`是一个基于`akka`架构上构建的分布式ETL调度工具，可以把任务拆分在集群中不同的节点上运行，高效利用集群资源，监控文件数据情况，对数据及任务进行监控告警，异常处理。其中工作流定义参考`Oozie`，相对简洁轻量级，可作为构建数据仓库、或大数据平台上的调度工具。
整个`akkaflow`架构目前包含有三个节点角色：Master、Worker、Http-Server，每个角色可以独立部署
