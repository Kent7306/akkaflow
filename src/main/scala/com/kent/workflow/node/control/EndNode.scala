package com.kent.workflow.node.control

import com.kent.workflow.node.NodeInstance
import java.sql.Connection

import com.kent.workflow.node.Node
import java.sql.ResultSet

import com.kent.workflow.Workflow

class EndNode(name: String) extends ControlNode(name){
  def toJson(): String = "{}"

  def checkIntegrity(wf: Workflow): Unit = {}
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