package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNode
import com.kent.workflow.actionnode.TransferNode.ConnectType
import com.kent.workflow.actionnode.TransferNode._
import com.kent.pub.DeepCloneable

/*
<transfer>
    <source type="MYSQL" jdbc-url="xxxx" username="xxx" password="xxxx">order_item</source>
    <source type="FILE" path="/home/kent/info.txt" delimited="\t"></source>
    
    <target type="MYSQL" jdbc-url="xxxx" username="xxxx" password="xxx" is-pre-truncate="true" table="order_item">
    	<pre-sql>delete from order_item where ds = '${param:stadate}'</pre-sql>
    	<after-sql>drop table order_item</after-pre>
    </target>
    <target type="LFS" path="/home/kent/info.txt" delimited="\t" is-pre-del="true"></target>
</transfer>
 */
/**
 * 数据传输节点
 */
class TransferNode(name: String) extends ActionNode(name: String) {
  var sourceInfo: SourceInfo = _
  var targetInfo: TargetInfo = _
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
    if(sConnectType == ConnectType.ORACLE || sConnectType == ConnectType.MYSQL){
      val jUrl = sNode.attribute("jdbc-url").get.text
      val uName = sNode.attribute("username").get.text
      val pwd = sNode.attribute("password").get.text
      val queryOrTable = sNode.text
      node.sourceInfo = SourceJdbcInfo(sConnectType, jUrl, uName, pwd, queryOrTable)
    }else if(sConnectType == ConnectType.LFS){
      val path = sNode.attribute("path").get.text
      val delimited = if(sNode.attribute("delimited").isDefined) sNode.attribute("delimited").get.text else "\t"
      node.sourceInfo = SourceFileInfo(sConnectType, path, delimited)
    }
    //target标签处理
    var targetSeq = (xmlNode \ "target")
    if(targetSeq.size != 1){
      throw new Exception("未配置或配置多于一个<target>标签")
    }
     val tNode = targetSeq(0)
    val tConnectType = ConnectType.withName(tNode.attribute("type").get.text.toUpperCase())
    if(tConnectType == ConnectType.ORACLE || tConnectType == ConnectType.MYSQL){
      val jUrl = tNode.attribute("jdbc-url").get.text
      val uName = tNode.attribute("username").get.text
      val pwd = tNode.attribute("password").get.text
      val isPreTrun = if(tNode.attribute("is-pre-truncate").isDefined) tNode.attribute("is-pre-truncate").get.text.toBoolean else false
      val table = tNode.attribute("table").get.text
      val preSql = if((tNode \ "pre-sql").size > 0) (tNode \ "pre-sql")(0).text else null
      val afterSql = if((tNode \ "after-sql").size > 0) (tNode \ "after-sql")(0).text else null
      node.targetInfo = TargetJdbcInfo(tConnectType, jUrl, uName, pwd, isPreTrun, table, preSql, afterSql)
    }else if(tConnectType == ConnectType.LFS){
      val path = tNode.attribute("path").get.text
      val delimited = if(tNode.attribute("delimited").isDefined) tNode.attribute("delimited").get.text else "\t"
      val isPreDel = if(tNode.attribute("is-pre-del").isDefined) tNode.attribute("is-pre-del").get.text.toBoolean else false
      node.targetInfo = TargetFileInfo(tConnectType, path, isPreDel , delimited)
    }
    node
  }
  
  object ConnectType extends Enumeration {
    type ConnectType = Value
    val HIVE, ORACLE, MYSQL, LFS, HDFS = Value
  }
  import com.kent.workflow.actionnode.TransferNode.ConnectType._
  class SourceInfo(val sType: ConnectType) extends DeepCloneable[SourceInfo]
  class TargetInfo(val sType: ConnectType) extends DeepCloneable[TargetInfo]
  case class SourceJdbcInfo(override val sType: ConnectType, jdbcUrl: String, username: String, password: String, 
                            tableSql: String) extends SourceInfo(sType)
  case class SourceFileInfo(override val sType: ConnectType, path: String,delimited: String) extends SourceInfo(sType)
  case class TargetJdbcInfo(override val sType: ConnectType, jdbcUrl: String, username: String, password: String, 
                           isPreTruncate: Boolean, table: String, 
                           preSql: String,afterSql: String) extends TargetInfo(sType)
  case class TargetFileInfo(override val sType: ConnectType, path: String, isPreDel: Boolean, delimited: String) extends TargetInfo(sType)
  
  //Event
  case class GetColNum()
  case class ColNum(colNum: Int)
  
  case class GetRows()
  case class Rows(rows: List[List[String]])
}