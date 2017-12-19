package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNode
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet

class EndNode(name: String) extends ControlNode(name){
  def getJson(): String = "{}"
}

object EndNode {
  def apply(name: String): EndNode = new EndNode(name)
  def apply(node: scala.xml.Node): EndNode = {
    val nameOpt = node.attribute("name")
    if(nameOpt == None){
      throw new Exception("节点<end/>未配置name属性")
    }
    val en = EndNode(nameOpt.get.text)
    en
  }
}