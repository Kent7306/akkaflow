package com.kent.workflow.node.control

import com.kent.workflow.node.NodeInstance
import java.sql.Connection

import com.kent.workflow.node.Node
import java.sql.ResultSet

import com.kent.util.Util
import org.json4s.jackson.JsonMethods
import com.kent.workflow.Workflow

class JoinNode(name: String) extends ControlNode(name)  {
  var to: String = _
  
  override def toJson(): String = {
    s"""{"to":"${to}"}"""
  }

  def checkIntegrity(wf: Workflow): Unit = {
    if(wf.nodeList.filter{_.name == to}.size == 0){
      throw new Exception(s"指向下一个节点${to}不存在")
    }
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