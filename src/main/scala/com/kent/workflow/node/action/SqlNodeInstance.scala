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
      case Some(dbLink) =>
        dbLink.executeSqls(sqlArr, (conn,stat) => {
        	  this.conn = conn
        	  this.stat = stat
        	}, infoLine => infoLog(infoLine), errorLine => errorLog(errorLine))
    }
    true
  }

  def kill(): Boolean = {
    try{
    if(stat != null) stat.cancel()
    if(conn != null) conn.close()
    } catch{
      case e: Exception => errorLog(s"KILL error! ${e}")
    }
    true
  }
}