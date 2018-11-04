package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNode
import com.kent.workflow.actionnode.transfer.source.Source.ConnectType
import com.kent.workflow.actionnode.TransferNode._
import com.kent.pub.DeepCloneable
import com.kent.workflow.actionnode.transfer.source.Source
import com.kent.workflow.actionnode.transfer.target.Target

/*
<transfer>
    <source type="DB" db-link="local_mysql">order_item</source>
    <source type="FILE" delimited="\t">/home/kent/info.txt</source>
    
    <target type="DB" db-link="local_mysql" is-pre-truncate="true" table="order_item">
    	<pre>delete from order_item where ds = '${param:stadate}'</pre>
    	<after>drop table order_item</after>
    </target>
    <target type="FILE" delimited="\t" is-pre-del="false" path="/home/kent/info.txt">
    	<pre>xxxxxx</pre>
    	<after>xxxx</after>
    </target>
</transfer>
 */
/**
 * 数据传输节点
 */
class TransferNode(name: String) extends ActionNode(name: String) {
  var dbsInfOpt: Option[DBSourceInf] = None
  var fsInfOpt: Option[FileSourceInf] = None
  var dbtInfOpt: Option[DBTargetInf] = None
  var ftInfOpt: Option[FileTargetInf] = None
  var script:Option[String] = None
  
  def getJson(): String = {
    "{}"
  }
}

object TransferNode{
  def apply(name: String): TransferNode = new TransferNode(name)
  def apply(name:String, xmlNode: scala.xml.Node): TransferNode = {
    val node = TransferNode(name)
    
    var scriptSeq = (xmlNode \ "script")
    if(!scriptSeq.isEmpty){
      node.script = Some(scriptSeq.text)
      return node
    }
    
    //source标签处理
    var sourceSeq = (xmlNode \ "source")
    if(sourceSeq.size != 1){
      throw new Exception("未配置或配置多于一个<source>标签")
    }
    val sNode = sourceSeq(0)
    val sConnectType = ConnectType.withName(sNode.attribute("type").get.text.toUpperCase())
    if(sConnectType == ConnectType.DB){
      val dbLinkName = sNode.attribute("db-link").get.text
      val queryOrTable = sNode.text
      node.dbsInfOpt = Some(DBSourceInf(dbLinkName, queryOrTable))
    }else if(sConnectType == ConnectType.FILE){
      val path = sNode.text
      val delimited = if(sNode.attribute("delimited").isDefined) sNode.attribute("delimited").get.text else "\t"
      node.fsInfOpt = Some(FileSourceInf(delimited, path))
    } else {
      throw new Exception("<source/>的type属性配置错误")
    }
    //target标签处理
    var targetSeq = (xmlNode \ "target")
    if(targetSeq.size != 1){
      throw new Exception("未配置或配置多于一个<target>标签")
    }
     val tNode = targetSeq(0)
    val tConnectType = ConnectType.withName(tNode.attribute("type").get.text.toUpperCase())
    if(tConnectType == ConnectType.DB){
      val dbLinkName = tNode.attribute("db-link").get.text
      val isPreTrun = if(tNode.attribute("is-pre-truncate").isDefined) tNode.attribute("is-pre-truncate").get.text.toBoolean else false
      val table = tNode.attribute("table").get.text
      val preSql = if((tNode \ "pre").size > 0) (tNode \ "pre")(0).text else null
      val afterSql = if((tNode \ "after").size > 0) (tNode \ "after")(0).text else null
      node.dbtInfOpt = Some(DBTargetInf(table, dbLinkName, isPreTrun, preSql, afterSql))
    }else if(tConnectType == ConnectType.FILE){
      val path = tNode.attribute("path").get.text
      val delimited = if(tNode.attribute("delimited").isDefined) tNode.attribute("delimited").get.text else "\t"
      val isPreDel = if(tNode.attribute("is-pre-del").isDefined) tNode.attribute("is-pre-del").get.text.toBoolean else false
      val preCmd = if((tNode \ "pre").size > 0) (tNode \ "pre")(0).text else null
      val afterCmd = if((tNode \ "after").size > 0) (tNode \ "after")(0).text else null
      node.ftInfOpt = Some(FileTargetInf(path, delimited, isPreDel , preCmd, afterCmd))
    }else {
      throw new Exception("<target/>的type属性配置错误")
    }
    node
  }
  
  case class DBSourceInf(dbLinkName: String, query: String)
  case class FileSourceInf(delimited: String, path: String)
  
  case class DBTargetInf(table: String, dbLinkName: String, isPreTruncate: Boolean, preSql: String, afterSql: String)
  case class FileTargetInf(path: String, delimited: String, isPreDel: Boolean, preCmd: String, afterCmd: String)
}