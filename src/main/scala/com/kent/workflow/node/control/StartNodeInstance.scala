package com.kent.workflow.node.control

import com.kent.workflow.node.NodeInstance
import com.kent.workflow.WorkflowInstance
import java.sql.Connection

import org.json4s.jackson.JsonMethods

class StartNodeInstance(override val nodeInfo: StartNode) extends ControlNodeInstance(nodeInfo){

  def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = 
    wfi.nodeInstanceList.filter { _.nodeInfo.name == nodeInfo.to }.toList
}

object StartNodeInstance {
  def apply(startNode: StartNode): StartNodeInstance = new StartNodeInstance(startNode)
}