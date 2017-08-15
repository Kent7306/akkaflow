package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInfo
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import java.sql.ResultSet
import org.json4s.jackson.JsonMethods

class ShellNode(name: String) extends ActionNodeInfo(name) {
  var command: String = _

  def createInstance(workflowInstanceId: String): ShellNodeInstance = {
    val hsani = ShellNodeInstance(this.deepCloneAs[ShellNode])
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

object ShellNode {
  def apply(name: String): ShellNode = new ShellNode(name)
  def apply(name:String, node: scala.xml.Node): ShellNode = parseXmlNode(name, node)
  
  def parseXmlNode(name: String, node: scala.xml.Node): ShellNode = {
    val command = (node \ "command")(0).text
    
	  val hsan = ShellNode(name)
	  hsan.command = command
	  hsan
  }
}