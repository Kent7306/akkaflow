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

  def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = List()
  
  override def terminate(wfa: WorkflowActor): Boolean = {
    wfa.terminateWith(W_SUCCESSED, "工作流实例成功执行完毕。")
    true
  }
}

object EndNodeInstance {
  def apply(endNode: EndNodeInfo): EndNodeInstance = new EndNodeInstance(endNode)
}