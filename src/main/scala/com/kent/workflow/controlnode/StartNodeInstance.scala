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

  override def setContent(contentStr: String){
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    val to = (content \ "to").extract[String]
    this.nodeInfo.to = to
  }
  
  override def getContent(): String = {
    s"""{"to":"${nodeInfo.to}"}"""
  }

}

object StartNodeInstance {
  def apply(startNode: StartNodeInfo): StartNodeInstance = {
    val sn = startNode.deepClone()
    val sni = new StartNodeInstance(sn)
    sni
  }
}