package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import java.sql.ResultSet
import org.json4s.jackson.JsonMethods

class ShellActionNodeInfo(name: String) extends ActionNodeInfo(name) {
  var command: String = _

  def deepClone(): ShellActionNodeInfo = {
    val fn = ShellActionNodeInfo(name)
    deepCloneAssist(fn)
    fn
  }
  def deepCloneAssist(hn: ShellActionNodeInfo): ShellActionNodeInfo = {
    super.deepCloneAssist(hn)
    hn.command = command
    hn
  }

  def createInstance(workflowInstanceId: String): ShellActionNodeInstance = {
    val hsani = ShellActionNodeInstance(this)
    hsani.id = workflowInstanceId
    hsani
  }

  override def setContent(contentStr: String){
	  super.setContent(contentStr)
    val content = JsonMethods.parse(contentStr)
    import org.json4s._
    implicit val formats = DefaultFormats
    val command = (content \ "command").extract[String]
    this.command = command
  }
  
  override def getContent(): String = {
    val contentStr = super.getContent()
    val c1 = JsonMethods.parse(contentStr)
    val c2 = JsonMethods.parse(s""" {"command":"${command}"}""")
    val c3 = c1.merge(c2)
    JsonMethods.pretty(JsonMethods.render(c3))
  }
}

object ShellActionNodeInfo {
  def apply(name: String): ShellActionNodeInfo = new ShellActionNodeInfo(name)
  def apply(name:String, node: scala.xml.Node): ShellActionNodeInfo = parseXmlNode(name, node)
  
  def parseXmlNode(name: String, node: scala.xml.Node): ShellActionNodeInfo = {
    val command = (node \ "command")(0).text
    
	  val hsan = ShellActionNodeInfo(name)
	  hsan.command = command
	  hsan
  }
}