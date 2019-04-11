package com.kent.workflow.node.control

import com.kent.util.Util
import com.kent.workflow.WorkflowActor
import com.kent.workflow.node.Node.Status._
import com.kent.workflow.node.NodeInstance

abstract class ControlNodeInstance(override val nodeInfo: ControlNode) extends NodeInstance(nodeInfo){
  def execute(): Boolean = true
  def terminate(wfa: WorkflowActor): Boolean = {
    this.endTime = Util.nowDate
	  this.changeStatus(SUCCESSED)
	  //查找下一节点
	  wfa.getNextNodesToWaittingQueue(this)
	  true
  }
  def replaceParam(param: Map[String, String]): Boolean = true
}