package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNode
import com.kent.workflow.actionnode.DataMonitorNode.SourceType
import org.json4s.reflect.`package`.SourceType
import com.kent.util.Util._

class SqlNode(name: String) extends ActionNode(name: String){
  import com.kent.workflow.actionnode.DataMonitorNode.SourceType._
  var sType: SourceType = _
  var jdbcUrl: String = _
  var username: String = _
  var password: String = _
  var sqls: String = _
  
  def getJson(): String = {
    s"""{
	        "type":${transJsonStr(sType.toString())},
	        "jdbc-url":${transJsonStr(jdbcUrl)},
	        "username":${transJsonStr(username)},
	        "password":${transJsonStr(password)},
	        "sqls":${transJsonStr(sqls)}
	      }"""
  }
}

object SqlNode{
  def apply(name: String): SqlNode = new SqlNode(name)
  def apply(name:String, xmlNode: scala.xml.Node): SqlNode = {
    val node = SqlNode(name)
    //type
    val sTypeOpt = xmlNode.attribute("type")
    if(sTypeOpt.isEmpty) throw new Exception(s"节点[sql: ${name}] 未配置type属性")
    val sType = SourceType.withName(sTypeOpt.get.text)
    if(sType != SourceType.HIVE && sType != SourceType.ORACLE && sType != SourceType.MYSQL){
      throw new Exception(s"节点[sql: ${name}] 属性type配置错误")
    }
    node.sType = sType
    //jdbc-url
    val jdbcUrlOpt = xmlNode.attribute("jdbc-url")
    if(jdbcUrlOpt.isEmpty) throw new Exception(s"节点[sql: ${name}] 未配置jdbc-url属性")
    node.jdbcUrl = jdbcUrlOpt.get.text
    //username
    val usernameOpt = xmlNode.attribute("username")
    if(usernameOpt.isEmpty) throw new Exception(s"节点[sql: ${name}] 未配置username属性")
    node.username = usernameOpt.get.text
    //password
    val passwordOpt = xmlNode.attribute("password")
    if(passwordOpt.isEmpty) throw new Exception(s"节点[sql: ${name}] 未配置password属性")
    node.password = passwordOpt.get.text
	  //sqls
    node.sqls = xmlNode.text
    node
  }
}