package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet

class EndNodeInfo(name: String) extends ControlNodeInfo(name){

  override def createInstance(workflowInstanceId: String): EndNodeInstance = {
    val eni = EndNodeInstance(this.deepCloneAs[EndNodeInfo])
    eni.id = workflowInstanceId
    eni
  }
}

object EndNodeInfo {
  def apply(name: String): EndNodeInfo = new EndNodeInfo(name)
  def apply(node: scala.xml.Node): EndNodeInfo = parseXmlNode(node)
  /**
   * 解析xml为一个对象
   */
  def parseXmlNode(node: scala.xml.Node): EndNodeInfo = {
    val nameOpt = node.attribute("name")
    if(nameOpt == None){
      throw new Exception("节点<end/>未配置name属性")
    }
    val en = EndNodeInfo(nameOpt.get.text)
    en
  }
}