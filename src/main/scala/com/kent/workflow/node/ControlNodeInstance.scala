package com.kent.workflow.node

import com.kent.workflow.WorkflowInstance
import com.kent.workflow.WorkflowActor
import com.kent.util.Util
import com.kent.workflow.node.NodeInfo.Status._

abstract class ControlNodeInstance(override val nodeInfo: ControlNodeInfo) extends NodeInstance(nodeInfo){
  def execute(): Boolean = true
  def terminate(wfa: WorkflowActor): Boolean = {
    this.endTime = Util.nowDate
	  this.status = SUCCESSED
	  //查找下一节点
	  val nodes = this.getNextNodes(wfa.workflowInstance)
	  nodes.filter { _.ifCanExecuted(wfa.workflowInstance) }.foreach { x => wfa.waitingNodes = wfa.waitingNodes.enqueue(x)}
	  true
  }
  
  override def toString(): String = {
	    var str = "  "+this.getClass.getName + "(\n"
	    str = str + s"    id = ${id},\n"
	    str = str + s"    name = ${nodeInfo.name},\n"
	    str = str + s"    status = ${status},\n"
	    str = str + s"    startTime = ${startTime},\n"
	    str = str + s"    endTime = ${endTime})\n"
	    str
	  }

  def replaceParam(param: Map[String, String]): Boolean = true

  def deepCloneAssist(e: ControlNodeInstance): ControlNodeInstance = {
    super.deepCloneAssist(e)
    e
  }
}