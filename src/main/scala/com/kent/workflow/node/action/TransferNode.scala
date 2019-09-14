package com.kent.workflow.node.action

import com.kent.workflow.node.action.transfer.source.Source.ConnectType
import com.kent.workflow.node.action.TransferNode._
import com.kent.pub.DeepCloneable
import com.kent.pub.io.FileLink
import com.kent.workflow.node.action.transfer.source.Source
import com.kent.workflow.node.action.transfer.target.Target

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
  * @param name
  */
class TransferNode(name: String) extends ActionNode(name: String) {
  var dbsInfOpt: Option[DBSourceInf] = None
  var fsInfOpt: Option[FileSourceInf] = None
  var dbtInfOpt: Option[DBTargetInf] = None
  var ftInfOpt: Option[FileTargetInf] = None
  var script:Option[String] = None
  
  def toJsonString(): String = {
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

      val flOpt = sNode.attribute("file-link")
      val fl = if (flOpt.isDefined) flOpt.get.text else FileLink.DEFAULT_FILE_LINK

      val delimited = if(sNode.attribute("delimited").isDefined) sNode.attribute("delimited").get.text else "\t"
      node.fsInfOpt = Some(FileSourceInf(fl, delimited, path))
    } else {
      throw new Exception("<source/>的type属性配置错误")
    }



    //target标签处理
    val tNode = xmlNode \ "target" match {
      case targetSeq if targetSeq.size != 1 => throw new Exception("未配置或配置多于一个<target>标签")
      case targetSeq => targetSeq.head
    }

    val tConnectType = ConnectType.withName(tNode.attribute("type").get.text.toUpperCase())
    if(tConnectType == ConnectType.DB){  //数据库系统

      val dbLinkName = tNode.attribute("db-link").get.text
      val isPreTrun = if(tNode.attribute("is-pre-truncate").isDefined) tNode.attribute("is-pre-truncate").get.text.toBoolean else false
      val table = tNode.attribute("table").get.text
      val preSql = if((tNode \ "pre").size > 0) (tNode \ "pre")(0).text else null
      val afterSql = if((tNode \ "after").size > 0) (tNode \ "after")(0).text else null

      val taskNum = tNode.attribute("task-num") match {
        case Some(taskNumSeq) if taskNumSeq(0).text.toInt <= 0 => throw new Exception("<target/>中task-num属性不能少于1")
        case Some(taskNumSeq) => taskNumSeq(0).text.toInt
        case None => 2
      }

      node.dbtInfOpt = Some(DBTargetInf(dbLinkName, taskNum, table, isPreTrun, preSql, afterSql))
    } else if(tConnectType == ConnectType.FILE){  //文件系统

      val path = tNode.attribute("path").get.text
      val delimited = if(tNode.attribute("delimited").isDefined) tNode.attribute("delimited").get.text else "\t"
      val isPreDel = if(tNode.attribute("is-pre-del").isDefined) tNode.attribute("is-pre-del").get.text.toBoolean else false

      val flOpt = tNode.attribute("file-link")
      val fl = if (flOpt.isDefined) flOpt.get.text else FileLink.DEFAULT_FILE_LINK

      val taskNum = tNode.attribute("task-num") match {
        case Some(taskNumSeq) if taskNumSeq.head.text.toInt <= 0 => throw new Exception("<target/>中task-num属性不能少于1")
        case Some(taskNumSeq) => 1  //??? 因为文件不能同时启几个任务去写入，所有只能为1个
        case None => 1
      }

      val preCmd = if((tNode \ "pre").nonEmpty) (tNode \ "pre").head.text else null
      val afterCmd = if((tNode \ "after").nonEmpty) (tNode \ "after").head.text else null
      node.ftInfOpt = Some(FileTargetInf(fl, taskNum, path, delimited, isPreDel , preCmd, afterCmd))
    }else {
      throw new Exception("<target/>的type属性配置错误")
    }
    node
  }
  
  case class DBSourceInf(dbLinkName: String, query: String)
  case class FileSourceInf(fileLinkName: String, delimited: String, path: String)
  
  case class DBTargetInf(dbLinkName: String, taskNum: Int,table: String, isPreTruncate: Boolean, preSql: String, afterSql: String)
  case class FileTargetInf(fileLinkName: String, taskNum: Int, path: String, delimited: String, isPreDel: Boolean, preCmd: String, afterCmd: String)
}