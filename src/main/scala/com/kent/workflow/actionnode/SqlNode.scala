package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNode
import com.kent.workflow.actionnode.DataMonitorNode.SourceType
import org.json4s.reflect.`package`.SourceType
import com.kent.util.Util._
import com.kent.pub.Event.DBLink

class SqlNode(name: String) extends ActionNode(name: String){
  import com.kent.workflow.actionnode.DataMonitorNode.SourceType._
  var dbLinkName: String = _
  var sqls: String = _
  
  def getJson(): String = {
    s"""{
	        "db-link":${transJsonStr(dbLinkName)},
	        "sqls":${transJsonStr(sqls)}
	      }"""
  }
}

object SqlNode{
  def apply(name: String): SqlNode = new SqlNode(name)
  def apply(name:String, xmlNode: scala.xml.Node): SqlNode = {
    val node = SqlNode(name)
    //db-link
    val dblOpt = xmlNode.attribute("db-link")
    if(dblOpt.isEmpty) throw new Exception(s"节点[sql: ${name}] 未配置db-link属性")
    node.dbLinkName = dblOpt.get.text
	  //sqls
    node.sqls = xmlNode.text
    node
  }
}