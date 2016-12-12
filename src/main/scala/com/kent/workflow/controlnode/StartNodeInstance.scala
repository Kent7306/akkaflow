package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInstance
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.WorkflowInstance
import java.sql.Connection
import org.json4s.jackson.JsonMethods

class StartNodeInstance(override val nodeInfo: StartNodeInfo) extends ControlNodeInstance(nodeInfo){
  
  def deepClone(): StartNodeInstance = {
    val sni = StartNodeInstance(this.nodeInfo)
    deepCloneAssist(sni)
    sni
  }
  def deepCloneAssist(sni: StartNodeInstance): StartNodeInstance = {
     super.deepCloneAssist(sni)
     sni
  }

  def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = wfi.nodeInstanceList.filter { _.nodeInfo.name == nodeInfo.to }.toList
}

object StartNodeInstance {
  def apply(startNode: StartNodeInfo): StartNodeInstance = {
    val sn = startNode.deepClone()
    val sni = new StartNodeInstance(sn)
    sni
  }
}