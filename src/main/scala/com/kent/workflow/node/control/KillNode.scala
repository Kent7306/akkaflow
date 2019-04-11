package com.kent.workflow.node.control

import com.kent.workflow.node.NodeInstance
import java.sql.Connection

import com.kent.workflow.node.Node
import java.sql.ResultSet

import com.kent.util.Util
import org.json4s.jackson.JsonMethods
import com.kent.workflow.Workflow

class KillNode(name: String) extends ControlNode(name) {
  var msg: String = _
  
  override def toJson(): String = {
    s"""{"msg":"${msg}"}"""
  }

  def checkIntegrity(wf: Workflow): Unit = {}
}

object KillNode {
  def apply(name: String): KillNode = new KillNode(name)
  def apply(node: scala.xml.Node): KillNode = {
      val nameOpt = node.attribute("name")
      val msg = (node \ "message").text
      if(nameOpt == None){
        throw new Exception("节点<kill/>未配置name属性")
      }
      val kn = KillNode(nameOpt.get.text)
      kn.msg = msg
      kn
  }
}