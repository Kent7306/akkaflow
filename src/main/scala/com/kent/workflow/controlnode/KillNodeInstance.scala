package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInstance
import com.kent.workflow.WorkflowInstance
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.WorkflowActor
import com.kent.workflow.WorkflowInfo.WStatus._
import com.kent.workflow.node.NodeInfo.Status._
import com.kent.coordinate.ParamHandler
import java.util.Date
import org.json4s.jackson.JsonMethods

class KillNodeInstance (override val nodeInfo: KillNodeInfo) extends ControlNodeInstance(nodeInfo){

  def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = List()
  
  override def terminate(wfa: WorkflowActor): Boolean = {
    println("KILL! 执行workflow名称："+wfa.workflowInstance.workflow.name+"执行完毕."+"actor名称: "+ wfa.workflowInstance.actorName)
    wfa.workflowInstance.status = W_KILLED
    this.status = SUCCESSED
    wfa.killRunningNodeActors(_.terminate())
    true
  }

  override def replaceParam(param: Map[String, String]): Boolean = {
    this.nodeInfo.msg = ParamHandler(new Date()).getValue(nodeInfo.msg)
    true
  }
  
   override def toString(): String = {
	    var str = "  "+this.getClass.getName + "(\n"
	    str = str + s"    id = ${id},\n"
	    str = str + s"    name = ${nodeInfo.name},\n"
	    str = str + s"    status = ${status},\n"
	    str = str + s"    startTime = ${startTime},\n"
	    str = str + s"    endTime = ${endTime})\n"
	    str = str + s"    executedMsg = ${executedMsg}\n"
	    str = str + s"    msg = ${nodeInfo.msg}\n"
	    str
	  }
}

object KillNodeInstance {
  def apply(killNode: KillNodeInfo): KillNodeInstance = new KillNodeInstance(killNode)
}