package com.kent.workflow.node

import com.kent.workflow.controlnode.StartNodeInfo
import com.kent.workflow.controlnode.EndNodeInfo
import com.kent.workflow.controlnode.ForkNodeInfo
import com.kent.workflow.controlnode.JoinNodeInfo
import com.kent.workflow.controlnode.KillNodeInfo

abstract class ControlNodeInfo(name: String) extends NodeInfo(name) {
  
}

object ControlNodeInfo{
  def apply(node: scala.xml.Node): ControlNodeInfo = parseXmlNode(node)
  
  def parseXmlNode(node: scala.xml.Node): ControlNodeInfo = {
    var controlNode: ControlNodeInfo = null
    node match {
      case <start/> => 
        controlNode = StartNodeInfo(node)
      case <end/> =>
        controlNode = EndNodeInfo(node)
      case <kill>{contents @_*}</kill> =>
			  controlNode = KillNodeInfo(node)
      case <fork>{contents @_*}</fork> =>
        controlNode = ForkNodeInfo(node)
      case <join/> =>
        controlNode = JoinNodeInfo(node)
      case _ =>
        throw new Exception("控制节点不存在:" + node)
   }
   val descOpt = node.attribute("desc")
   if(descOpt != None) controlNode.desc = descOpt.get.text
   controlNode
  }
}