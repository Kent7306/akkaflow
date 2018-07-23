package com.kent.workflow

import com.kent.workflow.WorkflowInfo.WStatus._
import com.kent.workflow.node.NodeInfo.Status._
import com.kent.pub.DeepCloneable
import com.kent.workflow.node.NodeInstance
import java.util.Date
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.controlnode.StartNodeInstance
import com.kent.util.Util
import com.kent.pub.Daoable
import java.sql.Connection
import java.sql.ResultSet
import org.json4s.jackson.JsonMethods
import org.json4s.JsonAST.JObject
import org.json4s.JsonAST.JString
import java.sql.SQLException
import com.kent.workflow.WorkflowInfo.WStatus
import akka.actor.ActorRef
import com.kent.db.PersistManager
import com.kent.pub.Directory
import com.kent.coordinate.ParamHandler
import com.kent.pub.Event._
import com.kent.main.Master
import akka.pattern.{ ask, pipe }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import akka.util.Timeout
import scala.concurrent.duration._
import com.kent.pub.Persistable


class WorkflowInstance(val workflow: WorkflowInfo) extends DeepCloneable[WorkflowInstance] with Persistable[WorkflowInstance] {
  var id: String = _
  def actorName = s"${id}"
  var paramMap:Map[String, String] = Map()
  var startTime: Date = _
  var endTime: Date = _
  private var status: WStatus = W_PREP     
  var nodeInstanceList:List[NodeInstance] = List()
  //是否完成时，自动触发下个依赖
  var isAutoTrigger = true
  /**
   * 修改工作流状态
   */
  def changeStatus(status: WStatus) = {
    this.status = status
    if(Master.persistManager!=null) Master.persistManager ! Save(this.deepClone())
  }
  def getStatus():WStatus = this.status
   /**
   * 得到工作流的开始节点
   */
  def getStartNode():Option[NodeInstance] = {
	  val sn = nodeInstanceList.filter { _.isInstanceOf[StartNodeInstance] }.toList
	  if(sn.size == 1) Some(sn(0)) else None
  }
  /**
   * 把当前工作流重置为未执行前状态
   */
  def reset(){
    this.status = W_PREP
    this.startTime = null
    this.endTime = null
    this.nodeInstanceList.foreach {ni => ni.reset()}
  }
  /**
   * 拼接工作流完成后的信息邮件
   */
  def htmlMail(relateWfs: List[WorkflowInfo])(implicit timeout:Timeout): Future[String] = {
    val part1 = s"""
<style> 
  .table-n {text-align: center; border-collapse: collapse;border:1px solid black}
  .table-n th{background-color: #d0d0d0}
  
  /*.warn {width:50px;background-color: orange;margin-right: 5px;border-radius:4px;text-align: center;float: left}
  .error {width:50px;background-color: red;margin-right: 5px;border-radius:4px;text-align: center;float: left}
  .info {width:50px;background-color: #349bfc;margin-right: 5px;border-radius:4px;text-align: center;float: left}*/
  .warn {color:orange;}
  .error {color:red;}
  .info {color:blue;}
  pre {margin:3px;}
  h3 {margin-bottom: 5px}
</style> 
      """
    val part2 = s"""
<h3>实例执行基本信息</h3>
<table>
	<tr><td width="100">实例ID</td><td>${this.id}</td></tr>
	<tr><td>工作流</td><td>${this.workflow.name}</td></tr>
	<tr><td>运行状态</td><td>${WStatus.getStatusName(this.getStatus())}</td></tr>
	<tr><td>开始时间</td><td>${Util.formatStandarTime(this.startTime)}</td></tr>
	<tr><td>运行时长</td><td>${((this.endTime.getTime-this.startTime.getTime)/1000).toInt}</td></tr>
	<tr><td>目录</td><td>${this.workflow.dir.dirname}</td></tr>
	<tr><td>参数</td><td>${this.paramMap.map{case(k,v) => s"$k:$v"}.mkString(", ")}</td></tr>
	<tr><td>告警级别</td><td>${this.workflow.mailLevel.mkString(",")}</td></tr>
	<tr><td>收件人员</td><td>${this.workflow.mailReceivers.mkString(",")}</td></tr>
	<tr><td>描述</td><td>${this.workflow.desc}</td></tr>
</table>
      """
	
	val strTmp3 = relateWfs.map { x => s"""
	           <tr>
	           <td>${x.name}</td><td>${x.creator}</td><td>${x.mailReceivers.mkString(",")}</td>
	           <td>${x.dir.dirname}</td><td>${x.desc}</td>
	           </tr>""" }.toList.mkString("\n")
	val part5 = s"""
	  <h3>后置触发受影响的工作流列表</h3>
	  <table class="table-n" border="1">
  	<tr>
  		<th>工作流名称</th><th>创建者</th><th>收件人</th><th>目录</th><th>描述</th>
  	</tr>
  	${strTmp3}
	  </table>
	  """
	
	
	
	import com.kent.workflow.node.NodeInfo.Status
	val nodesStr = this.nodeInstanceList.map { x => s"""
	  <tr>
	    <td>${x.nodeInfo.name}</td>
	    <td>${x.nodeInfo.getType}</td>
	    <td>${Status.getStatusName(x.status)}</td>
	    <td>${Util.formatStandarTime(x.startTime)}</td>
	    <td>${if(x.startTime == null || x.endTime == null) "--" 
	      else ((x.endTime.getTime-x.startTime.getTime)/1000).toInt}</td>
	    <td>${x.executedMsg}</td>
	    <td>${x.nodeInfo.desc}</td>
	  </tr>
	  """ }.mkString("\n")
	
      val part3 = s"""
<h3>节点执行信息</h3>
<table class="table-n" border="1">
	<tr>
		<th>节点名称</th><th>节点类型</th><th>节点状态</th><th>开始时间</th><th>运行时长</th><th>执行信息</th><th>描述</th>
	</tr>
	${nodesStr}
</table>
        """
	  val logListF = (Master.logRecorder ? GetLog(null,this.id, null)).mapTo[List[List[String]]]
	  logListF.map { rows => 
	     val logLines = rows.map { cols => 
	       val aDom = cols(0).toUpperCase() match {
    	      case "INFO" => "<a style='color:blue;margin-right:15px;font-weight:bolder'>INFO    </a>"
    	      case "WARN" => "<a style='color:orange;margin-right:15px;font-weight:bolder'>WARN  </a>"
    	      case "ERROR" => "<a style='color:red;margin-right:15px;font-weight:bolder'>ERROR</a>"
    	      case other => s"<a style='color:black;margin-right:15px;font-weight:bolder'>${other}</a>"
    	    }
	       s"""<pre>${aDom}[${cols(1)}][${cols(2)}] ${cols(3)}</pre>"""
	     }.mkString("\n")
	     val part4 = s"""
	       <h3>执行日志</h3>
	       $logLines
	       """
	     val html = part1 + part2 + part3 + part5 + part4
	     html
	  }
  }
  
  def save(implicit conn: Connection): Boolean = {
    var result = false;
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    import com.kent.util.Util._
    val paramStr = compact(render(paramMap))
    val levelStr = compact(workflow.mailLevel.map { _.toString()})
    val receiversStr = compact(workflow.mailReceivers)
	  val insertSql = s"""
	     insert into workflow_instance values(${withQuate(id)},${withQuate(workflow.name)},${withQuate(workflow.creator)},${withQuate(workflow.dir.dirname)},
	                                          ${withQuate(paramStr)},'${status.id}',${withQuate(workflow.desc)},
	                                          ${withQuate(levelStr)},${withQuate(receiversStr)},${workflow.instanceLimit},
	                                          ${withQuate(formatStandarTime(startTime))},${withQuate(formatStandarTime(endTime))},
	                                          ${withQuate(formatStandarTime(workflow.createTime))},${withQuate(formatStandarTime(workflow.createTime))},
	                                          ${withQuate(transformXmlStr(workflow.xmlStr))})
	    """
	  val updateSql = s"""
	    update workflow_instance set
	                        status = '${status.id}',
	                        stime = ${withQuate(formatStandarTime(startTime))}, 
	                        etime = ${withQuate(formatStandarTime(endTime))}
	    where id = '${id}'
	    """
	    
  	try{
      conn.setAutoCommit(false)
  	  if(this.getEntityWithNodeInstance(false).isEmpty){
      	result = executeSql(insertSql)     
      }else{
        result = executeSql(updateSql)
      }
  	  //覆盖实例的节点
  		this.nodeInstanceList.foreach { _.save }
  	  conn.commit()
    }catch{
      case e: SQLException => e.printStackTrace(); conn.rollback();false
    }finally{
    	conn.setAutoCommit(true) 
    }
	  result
  }
  /**
   * 
   */
  def getEntity(implicit conn: Connection): Option[WorkflowInstance] = {
    getEntityWithNodeInstance(true)
  }
  @throws(classOf[Exception])
  def getEntityWithNodeInstance(isWithNodeInstance: Boolean)(implicit conn: Connection): Option[WorkflowInstance] = {
    import com.kent.util.Util._
    
    //工作流实例查询sql
    val queryStr = s"""
         select * from workflow_instance where id=${withQuate(id)}
                    """
    //节点实例查询sql
    val queryNodesStr = s"""
         select * from node_instance where workflow_instance_id = ${withQuate(id)}
      """
    
    val wfiOpt = querySql(queryStr, (rs: ResultSet) => {
      if(rs.next()){
        val xmlStr = rs.getString("xml_str")
        val json = JsonMethods.parse(rs.getString("param"))
        val list = for{ JObject(ele) <- json; (k, JString(v)) <- ele} yield (k->v)
        val parsedParams = list.map(x => x).toMap
        
        val wf = WorkflowInfo(xmlStr)
        val wfi = WorkflowInstance(wf, parsedParams)
        wfi.id = id
        wfi.status = WStatus.getWstatusWithId(rs.getInt("status"))
        wfi.startTime = Util.getStandarTimeWithStr(rs.getString("stime"))
        wfi.endTime = Util.getStandarTimeWithStr(rs.getString("etime"))
        wfi
      }else{
        null
      }
    })
    //关联查询节点实例
    if(isWithNodeInstance && !wfiOpt.isEmpty){
      import com.kent.workflow.node.NodeInfo.Status
      querySql(queryNodesStr, (rs: ResultSet) => {
        var newNIList = List[NodeInstance]()
        while(rs.next()) {
        	val name = rs.getString("name")
        	val executeMsg = rs.getString("msg")
        	val startTime = Util.getStandarTimeWithStr(rs.getString("stime"))
        	val endTime = Util.getStandarTimeWithStr(rs.getString("etime"))
        	val status = Status.getStatusWithId(rs.getInt("status")) 
        	
         wfiOpt.get.workflow.nodeList.filter { x => x.name == rs.getString("name") }.foreach { x => 
            val ni = x.createInstance(this.id)
            ni.executedMsg = executeMsg
            ni.startTime = startTime
            ni.endTime = endTime
            ni.changeStatus(status)
            newNIList = newNIList :+ ni
          }
        }
        wfiOpt.get.nodeInstanceList = newNIList
        wfiOpt.get
      })
      wfiOpt
    }else{
      wfiOpt
    }
  }
  def delete(implicit conn: Connection): Boolean = {
    var result = false
    try{
      conn.setAutoCommit(false)
      executeSql(s"delete from log_record where id = '${id}'")
	    result = executeSql(s"delete from workflow_instance where id='${id}'")
	    executeSql(s"delete from node_instance where workflow_instance_id='${id}'")
	    conn.commit()
    }catch{
      case e: SQLException => e.printStackTrace();conn.rollback()
    }finally{
    	conn.setAutoCommit(true)
    }
    result
  }
}

object WorkflowInstance {
  /**
   * 由一个xml和转换前参数产生一个实例
   */
  def apply(id: String,xmlStr: String, paramMap: Map[String, String]): WorkflowInstance = {
    if(xmlStr != null && xmlStr.trim() != ""){
      val parseXmlStr = ParamHandler(Util.nowDate).getValue(xmlStr, paramMap)
      val parseWf = WorkflowInfo(parseXmlStr)
    	val wfi = new WorkflowInstance(parseWf)
      wfi.id = if(id == null || id.trim() == "") Util.produce8UUID else id
      wfi.paramMap = paramMap
    	wfi.nodeInstanceList = wfi.workflow.nodeList.map { _.createInstance(wfi.id) }.toList
    	wfi      
    } else{
      val wfi = new WorkflowInstance(null)
      wfi.id = if(id == null && id.trim() == "") Util.produce8UUID else id
      wfi.paramMap = paramMap
      wfi
    }
  }
  /**
   * 由一个workflow对象产生一个实例
   */
  def apply(wf: WorkflowInfo, parseParams: Map[String, String]): WorkflowInstance = WorkflowInstance(null, wf.xmlStr, parseParams)
  def apply(id: String, wf: WorkflowInfo, parseParams: Map[String, String]): WorkflowInstance = WorkflowInstance(id, wf.xmlStr, parseParams)
  /**
   * 由wfiid生成一个空壳的实例
   */
  def apply(id: String): WorkflowInstance = WorkflowInstance(id, "", Map[String, String]())
}
