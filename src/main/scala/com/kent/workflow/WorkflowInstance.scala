package com.kent.workflow

import java.util.Date

import akka.pattern.ask
import akka.util.Timeout
import com.kent.main.Master
import com.kent.pub.DeepCloneable
import com.kent.pub.Event._
import com.kent.pub.dao.WorkflowInstanceDao
import com.kent.util.{ParamHandler, Util}
import com.kent.workflow.Coor.TriggerType
import com.kent.workflow.Coor.TriggerType._
import com.kent.workflow.Workflow.WStatus
import com.kent.workflow.Workflow.WStatus._
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.node.control.StartNodeInstance

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * 工作流实例类
  * @param workflow
  */
class WorkflowInstance(val workflow: Workflow) extends DeepCloneable[WorkflowInstance] {
  var id: String = _
  def actorName = s"${id}"
  var paramMap:Map[String, String] = Map()
  var startTime: Date = _
  var endTime: Date = _
  var status: WStatus = W_PREP
  var nodeInstanceList:List[NodeInstance] = List()
  //是否完成时，自动触发下个依赖
  //var isAutoTrigger = true
  
  //后置触发类型
  var triggerType: TriggerType = TriggerType.NEXT_SET
  
  /**
   * 修改工作流状态
   */
  def changeStatus(status: WStatus) = {
    this.status = status
    WorkflowInstanceDao.update(this)
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
  def htmlMail(relateWfs: List[Workflow])(implicit timeout:Timeout): Future[String] = {
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
	
	
	
	import com.kent.workflow.node.Node.Status
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
}

object WorkflowInstance {
  /**
   * 由一个xml和转换前参数产生一个实例
   */
  def apply(id: String,xmlStr: String, paramMap: Map[String, String]): WorkflowInstance = {
    if(xmlStr != null && xmlStr.trim() != ""){
      val parseXmlStr = ParamHandler(Util.nowDate).getValue(xmlStr, paramMap)
      val parseWf = Workflow(parseXmlStr)
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
  def apply(wf: Workflow, parseParams: Map[String, String]): WorkflowInstance = WorkflowInstance(null, wf.xmlStr, parseParams)
  def apply(id: String, wf: Workflow, parseParams: Map[String, String]): WorkflowInstance = WorkflowInstance(id, wf.xmlStr, parseParams)
  /**
   * 由wfiid生成一个空壳的实例
   */
  def apply(id: String): WorkflowInstance = WorkflowInstance(id, "", Map[String, String]())
}
