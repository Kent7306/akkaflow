package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNode
import com.kent.workflow.node.NodeInstance
import java.sql.Connection
import com.kent.workflow.node.NodeInfo
import java.sql.ResultSet
import java.sql.ResultSet
import org.json4s.jackson.JsonMethods
/**
<!--example 1 -->
<shell>
   <command>cd /home/xxx; hdfs dfs -rm -r /xxx/xxx/xxxx</command>
</shell>
<!-- example 2 -->
<shell>
   <command>/home/tmp/run_data.sh xxx</command>
</shell>
 */
class ShellNode(name: String) extends ActionNode(name) {
  var command: String = _
  
  override def getJson(): String = {
    import com.kent.util.Util._
    s""" {"command":"${transJsonStr(command)}"}"""
  }
}

object ShellNode {
  def apply(name: String): ShellNode = new ShellNode(name)
  def apply(name:String, node: scala.xml.Node): ShellNode = {
    val command = (node \ "command")(0).text
	  val hsan = ShellNode(name)
	  hsan.command = command
	  hsan
  }
}