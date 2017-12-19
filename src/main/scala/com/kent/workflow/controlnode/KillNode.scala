package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNode
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import com.kent.util.Util
import org.json4s.jackson.JsonMethods

class KillNode(name: String) extends ControlNode(name) {
  var msg: String = _
  
  override def getJson(): String = {
    s"""{"msg":"${msg}"}"""
  }
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