package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInstance
import com.kent.workflow.node.NodeInstance
import com.kent.workflow.WorkflowInstance
import org.json4s.jackson.JsonMethods

class ForkNodeInstance(override val nodeInfo: ForkNodeInfo) extends ControlNodeInstance(nodeInfo){
  override def deepClone(): ForkNodeInstance = {
    val fni = ForkNodeInstance(this.nodeInfo)
    deepCloneAssist(fni)
    fni
  }
  def deepCloneAssist(fni: StartNodeInstance): StartNodeInstance = {
     super.deepCloneAssist(fni)
     fni
  }

  
  override def getNextNodes(wfi: WorkflowInstance): List[NodeInstance] = 
     wfi.nodeInstanceList.filter { x => nodeInfo.pathList.contains(x.nodeInfo.name) }.toList
     
  override def setContent(contentStr: String){
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    this.nodeInfo.pathList = (content \ "paths" \\ classOf[JString]).asInstanceOf[List[String]]
  }
  
  override def getContent(): String = {
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val pathStr = compact(render(nodeInfo.pathList))
    s"""{"paths":${pathStr}}"""
  }
  
}

object ForkNodeInstance {
  def apply(forkNode: ForkNodeInfo): ForkNodeInstance = {
    val fn = forkNode.deepClone()
    val fni = new ForkNodeInstance(fn)
    fni
  }
}