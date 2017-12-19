package com.kent.workflow.node

import com.kent.workflow.controlnode.StartNode
import com.kent.workflow.controlnode.EndNode
import com.kent.workflow.controlnode.ForkNode
import com.kent.workflow.controlnode.JoinNode
import com.kent.workflow.controlnode.KillNode
import org.json4s.jackson.JsonMethods

abstract class ControlNode(name: String) extends NodeInfo(name) {
  def getCateJson(): String = "{}"
}

object ControlNode{
  def apply(node: scala.xml.Node): ControlNode = {
    var controlNode: ControlNode = null
    node match {
      case <start/> => 
        controlNode = StartNode(node)
      case <end/> =>
        controlNode = EndNode(node)
      case <kill>{contents @_*}</kill> =>
			  controlNode = KillNode(node)
      case <fork>{contents @_*}</fork> =>
        controlNode = ForkNode(node)
      case <join/> =>
        controlNode = JoinNode(node)
      case _ =>
        throw new Exception("控制节点不存在:" + node)
   }
   val descOpt = node.attribute("desc")
   if(descOpt != None) controlNode.desc = descOpt.get.text
   controlNode
  }
}