package com.kent.workflow.node

import java.util.Date
import com.kent.pub.DeepCloneable
import com.kent.pub.Daoable

abstract class NodeInfo(var name: String) extends DeepCloneable[NodeInfo] with Daoable[NodeInfo] with Serializable{
  import com.kent.workflow.node.NodeInfo.Status._
  var workflowId: String = _
  var desc: String = _
  def createInstance(workflowInstanceId: String): NodeInstance
  def deepClone(): NodeInfo
  override def deepCloneAssist(e: NodeInfo): NodeInfo = {
	    e.workflowId = workflowId
	    e.desc = desc
	    e
	  }
}

object NodeInfo {
  def apply(node: scala.xml.Node): NodeInfo = parseXmlNode(node)
  
  def parseXmlNode(node: scala.xml.Node): NodeInfo = {
    node match {
      case <action>{content @ _*}</action> => ActionNodeInfo(node)
      case _ => ControlNodeInfo(node)
    }
  }
  
  object Status extends Enumeration { 
    type Status = Value
    val PREP, RUNNING, SUSPENDED, SUCCESSED,FAILED,KILLED = Value
    def getStatusWithId(id: Int): Status = {
      var sta: Status = PREP  
      Status.values.foreach { x => if(x.id == id) return x }
      sta
    }
    
  }
  object NodeTagType  extends Enumeration{
    type  NodeTagType = Value
    val KILL = Value("kill")
    val START = Value("start")
    val END = Value("end")
    val JOIN = Value("join")
    val FORK = Value("fork")
    val HOST_SCRIPT = Value("host_cript")
    val SUB_WORKFLOW = Value("sub_workflow")
    val ACTION = Value("action")
  }
}