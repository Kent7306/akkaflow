package com.kent.workflow.node.control

import com.kent.workflow.WorkflowInstance
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.WorkflowActor
import com.kent.workflow.Workflow.WStatus._
import com.kent.workflow.node.Node.Status._
import java.util.Date

import com.kent.util.ParamHandler
import org.json4s.jackson.JsonMethods

class KillNodeInstance (override val nodeInfo: KillNode) extends ControlNodeInstance(nodeInfo){

  def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = List()
  
  override def terminate(wfa: WorkflowActor): Boolean = {
    wfa.terminateWith(W_KILLED, "执行到kill节点，主动杀死自己")
    true
  }

  override def replaceParam(param: Map[String, String]): Boolean = {
    this.nodeInfo.msg = ParamHandler(new Date()).getValue(nodeInfo.msg)
    true
  }
}

object KillNodeInstance {
  def apply(killNode: KillNode): KillNodeInstance = new KillNodeInstance(killNode)
}