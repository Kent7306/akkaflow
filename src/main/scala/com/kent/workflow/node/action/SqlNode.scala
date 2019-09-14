package com.kent.workflow.node.action

import com.kent.util.Util._

class SqlNode(name: String) extends ActionNode(name: String){
  var dbLinkName: String = _
  var sqls: String = _
  
  def toJsonString(): String = {
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