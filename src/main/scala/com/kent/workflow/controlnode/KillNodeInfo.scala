package com.kent.workflow.controlnode

import com.kent.workflow.node.ControlNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import com.kent.util.Util
import org.json4s.jackson.JsonMethods

class KillNodeInfo(name: String) extends ControlNodeInfo(name) {
  var msg: String = _

  override def createInstance(workflowInstanceId: String): KillNodeInstance = {
    val kni = KillNodeInstance(this.deepCloneAs[KillNodeInfo])
    kni.id = workflowInstanceId
    kni
  }

  override def setContent(contentStr: String){
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    val msg = (content \ "msg").extract[String]
    this.msg = msg
  }
  
  override def getContent(): String = {
    s"""{"msg":"${msg}"}"""
  }
}

object KillNodeInfo {
  def apply(name: String): KillNodeInfo = new KillNodeInfo(name)
  def apply(node: scala.xml.Node): KillNodeInfo = parseXmlNode(node)
  /**
   * 解析xml为一个对象
   */
  def parseXmlNode(node: scala.xml.Node): KillNodeInfo = {
      val nameOpt = node.attribute("name")
      val msg = (node \ "message").text
      if(nameOpt == None){
        throw new Exception("节点<kill/>未配置name属性")
      }
      val kn = KillNodeInfo(nameOpt.get.text)
      kn.msg = msg
      kn
  }
}