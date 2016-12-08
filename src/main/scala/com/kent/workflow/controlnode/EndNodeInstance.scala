package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInfo
import com.kent.workflow.node.ControlNodeInstance
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.WorkflowInstance
import com.kent.workflow.WorkflowActor
import com.kent.workflow.node.NodeInfo.Status._
import com.kent.workflow.WorkflowInfo.WStatus._
import com.kent.util.Util

class EndNodeInstance(override val nodeInfo: EndNodeInfo) extends ControlNodeInstance(nodeInfo) {
  
  def deepClone(): EndNodeInstance = {
    val eni = EndNodeInstance(this.nodeInfo)
    deepCloneAssist(eni)
    eni
  }
  def deepCloneAssist(eni: StartNodeInstance): StartNodeInstance = {
     super.deepCloneAssist(eni)
     eni
  }

  def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = List()
  
  override def terminate(wfa: WorkflowActor): Boolean = {
    this.status =  SUCCESSED
    this.endTime = Util.nowDate
    println("workflow名称："+wfa.workflowInstance.workflow.name+"执行完毕."+"actor名称: "+ wfa.workflowInstance.actorName)
    wfa.workflowInstance.status = W_SUCCESSED
    wfa.terminate()
    true
  }
}

object EndNodeInstance {
  def apply(endNode: EndNodeInfo): EndNodeInstance = {
    val en = endNode.deepClone()
    val eni = new EndNodeInstance(en)
    eni
  }
}