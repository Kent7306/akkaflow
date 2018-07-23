package com.kent.workflow.actionnode

import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager
import com.kent.workflow.node.ActionNodeInstance
import com.kent.workflow.actionnode.DataMonitorNode.SourceType
import org.apache.hive.jdbc.HiveStatement
import scala.collection.JavaConverters._
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader
import scala.concurrent.Await
import scala.concurrent.duration._
import com.kent.workflow.actionnode.DataMonitorNode.SourceType._
import com.kent.workflow.actionnode.DataMonitorNode.DatabaseType._
import com.kent.pub.Event.DBLink
import com.kent.pub.db.MysqlOpera
import com.kent.pub.db.HiveOpera
import com.kent.pub.db.OracleOpera

class SqlNodeInstance(override val nodeInfo: SqlNode) extends ActionNodeInstance(nodeInfo){
  var conn:Connection = null
  var stat:Statement = null
  
  def execute(): Boolean = {
    val sqlArr = nodeInfo.sqls.split(";").map { _.trim() }.filter { _ != "" }.toList
    val dbLinkOptF = this.actionActor.getDBLink(nodeInfo.dbLinkName)
    val dbLinkOpt = Await.result(dbLinkOptF, 20 seconds)
    if(dbLinkOpt.isEmpty){
      errorLog(s"[db-link:${nodeInfo.dbLinkName}]未配置")
      false
    }else if(dbLinkOpt.get.dbType == MYSQL){
      MysqlOpera.executeSqls(dbLinkOpt.get, sqlArr)
    }else if(dbLinkOpt.get.dbType == ORACLE){
      OracleOpera.executeSqls(dbLinkOpt.get, sqlArr)
    }else if(dbLinkOpt.get.dbType == HIVE){
      HiveOpera.executeSqls(sqlArr, dbLinkOpt.get, (conn,stat) => {
    	 this.conn = conn
    	 this.stat = stat
    	}, infoLine => infoLog(infoLine), errorLine => errorLog(errorLine))
    }else{
      throw new Exception(s"db-link未配置${dbLinkOpt.get.dbType}类型")
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