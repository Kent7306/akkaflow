package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInstance
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.WorkflowInstance
import org.json4s.jackson.JsonMethods

class ForkNodeInstance(override val nodeInfo: ForkNode) extends ControlNodeInstance(nodeInfo){
  
  override def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = 
     wfi.nodeInstanceList.filter { x => nodeInfo.pathList.contains(x.nodeInfo.name) }.toList
  
}

object ForkNodeInstance {
  def apply(forkNode: ForkNode): ForkNodeInstance = new ForkNodeInstance(forkNode)
}