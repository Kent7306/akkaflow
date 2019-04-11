package com.kent.workflow.node.action

import java.sql.{Connection, Statement}

import com.kent.pub.Event._
import com.kent.pub.db.DBLink.DatabaseType._
import com.kent.pub.db.{HiveOpera, MysqlOpera, OracleOpera}
import com.kent.util.Util

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class SqlNodeInstance(override val nodeInfo: SqlNode) extends ActionNodeInstance(nodeInfo){
  var conn:Connection = null
  var stat:Statement = null
  
  def execute(): Boolean = {
    val sqlArr = nodeInfo.sqls.split(";").map { _.trim() }.filter { _ != "" }.toList
    val dbLinkOptF = this.actionActor.getDBLink(nodeInfo.dbLinkName)
    val dbLinkOpt = Await.result(dbLinkOptF, 20 seconds)
    
    dbLinkOpt match {
      case None => 
        throw new Exception(s"[db-link:${nodeInfo.dbLinkName}]未配置")
      case Some(dbLink) if dbLink.dbType == MYSQL =>
        MysqlOpera.executeSqls(sqlArr, dbLink, (conn, stat) => {
          this.conn = conn
          this.stat = stat
        })
      case Some(dbLink) if dbLink.dbType == ORACLE =>
        OracleOpera.executeSqls(sqlArr, dbLink)
      case Some(dbLink) if dbLink.dbType == HIVE =>
        HiveOpera.executeSqls(sqlArr, dbLink, (conn,stat) => {
        	  this.conn = conn
        	  this.stat = stat
        	}, infoLine => infoLog(infoLine), errorLine => errorLog(errorLine))
       
      case Some(dbLink) =>
        throw new Exception(s"db-link未配置${dbLink.dbType}类型")
    }
    
    //保存血缘关系
    val isLineageEnabled = actionActor.context.system.settings.config.getBoolean("workflow.extra.lineage-enabled")
    
    if(isLineageEnabled){
      infoLog("开始保存血缘关系")
      val infoF = this.actionActor.getInstanceShortInfo()
      val info = Await.result(infoF, 20 seconds)
      
      val lineageRecords = 
      Try {
        val db = dbLinkOpt.get.dbType match {
          case MYSQL => MysqlOpera.getDBName(dbLinkOpt.get)
          case HIVE => HiveOpera.getDBName(dbLinkOpt.get)
          case ORACLE => OracleOpera.getDBName(dbLinkOpt.get) 
        }
        //这里暂时都用hive抽象语法树来做
        HiveOpera.getLineageRecords(sqlArr, db)
      }.recover{ 
        case e: Exception => 
          errorLog("解析血缘关系失败："+e.getMessage)
          List()
      }.get
      lineageRecords.foreach{lr => 
        lr.targetTable.workflowName = info.name
        lr.targetTable.lastUpdateTime = Util.nowDate
        lr.targetTable.dbLinkName = dbLinkOpt.get.name
        lr.targetTable.owner = info.owner
      }
      //打印
      lineageRecords.map{ case lr =>
          infoLog("目标表: "+lr.targetTable.name)
          val a = lr.relate.sourceTableNames.mkString(",")
          infoLog("源表:" + a)
      }
      //发送给master节点的lineaage-recorder actor进行保存
      val lineageRecorderPath = this.actionActor.workflowActorRef.path / ".." / ".." / "lineaage-recorder"
      val lineageRecorder = this.actionActor.context.actorSelection(lineageRecorderPath)
      lineageRecords.map{ lr =>
        lineageRecorder ! SaveLineageRecord(lr)      
      }
    }
    true
  }

  def kill(): Boolean = {
    try{
    if(stat != null) stat.cancel()
    if(conn != null) conn.close()
    } catch{
      case e: Exception => errorLog(s"KILL! ${e}")
    }
    true
  }
}