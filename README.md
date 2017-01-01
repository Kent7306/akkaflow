## Short Introduce
This is a distributed ETL dispatching system based on akka, it has three roles(http-server, master, worker) which can be deployed alone on various machines. But high availability has not yet been developed </br>
</br>
So far several modules are contained as follows: coordinator module, dispatch module, warning module, log module, persistence module. In some case it likes oozie, but more lightweight and easier. You can configure workflows and coordinator with xml file, eg. `wf.xml` and `coordinate.xml`.</br>
</br>
This project is just a back-end system, and is still under coding frequently, the document of how to deploy will be provided later. The GUI based on browser will be provided later soon (it has not been developed yet).</br>