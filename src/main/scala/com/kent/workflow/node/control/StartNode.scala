package com.kent.workflow.node.control

import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import java.sql.ResultSet

import org.json4s.jackson.JsonMethods
import com.kent.workflow.Workflow

class StartNode(name: String) extends ControlNode(name){
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

object StartNode {
  def apply(name: String): StartNode = new StartNode(name)
  def apply(node: scala.xml.Node):StartNode = {
      val nameOpt = node.attribute("name")
      val toOpt = node.attribute("to")
      if(nameOpt == None){
        throw new Exception("节点<start/>未配置name属性")
      }else if(toOpt == None){
        throw new Exception("节点<start/>未配置opt属性")
      }
      val sn = StartNode(nameOpt.get.text)
      sn.to = toOpt.get.text
      sn
  }
}