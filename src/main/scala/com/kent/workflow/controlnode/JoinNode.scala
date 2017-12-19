package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNode
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import com.kent.util.Util
import org.json4s.jackson.JsonMethods

class JoinNode(name: String) extends ControlNode(name)  {
  var to: String = _
  
  override def getJson(): String = {
    s"""{"to":"${to}"}"""
  }
}

object JoinNode {
  def apply(name: String): JoinNode = new JoinNode(name)
  def apply(node: scala.xml.Node): JoinNode = {
	  val nameOpt = node.attribute("name")
	  val toOpt = node.attribute("to")
    if((node \ "@name").size != 1){    
  	  throw new Exception("存在join未配置name属性")
    }
    val jn = JoinNode(nameOpt.get.text) 
    jn.to = toOpt.get.text
    jn
  }
}