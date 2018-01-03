package com.kent.workflow.actionnode

import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager
import com.kent.workflow.node.ActionNodeInstance
import com.kent.workflow.actionnode.DataMonitorNode.SourceType
import org.apache.hive.jdbc.HiveStatement
import scala.collection.JavaConverters._
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader

class SqlNodeInstance(override val nodeInfo: SqlNode) extends ActionNodeInstance(nodeInfo){
  var conn:Connection = null
  var stat:Statement = null
  
  def execute(): Boolean = {
    val sqlArr = nodeInfo.sqls.split(";").map { _.trim() }.filter { _ != "" }.toList
    
    val driverClsOpt = nodeInfo.sType match {
      case SourceType.MYSQL => Some("com.mysql.jdbc.Driver")
      case SourceType.ORACLE => Some("oracle.jdbc.driver.OracleDriver")
      case SourceType.HIVE => Some("org.apache.hive.jdbc.HiveDriver")
      case _ => None
    }
    if(driverClsOpt.isEmpty){
      errorLog(s"不支持${nodeInfo.sType}类型")
      return false
    }
    executeSqls(driverClsOpt.get, sqlArr, nodeInfo.jdbcUrl, nodeInfo.username, nodeInfo.password)
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
  
  /**
   * 获取rmdb数据
   */
  private def executeSqls(driverName: String, sqls: List[String], jdbcUrl: String, username: String, pwd: String):Boolean = {
    try{
  	  Class.forName(driverName)
  	  //得到连接
  	  conn = DriverManager.getConnection(jdbcUrl, username, pwd)
  	  conn.setAutoCommit(false)
  	  stat = conn.createStatement()
  	  //打印hive日志
  	  if(nodeInfo.sType == SourceType.HIVE){
  	    val logThread = new Thread(new Runnable() {
    			def run() {
    				val hivestat = stat.asInstanceOf[HiveStatement]
    				while (hivestat.hasMoreLogs())  {  
              hivestat.getQueryLog().asScala.map{ x => infoLog(x)}
              Thread.sleep(1000)  
            } 
    			}
    		});  
        logThread.setDaemon(true);  
        logThread.start();  
  	  }
  	  
    	val results = sqls.map { sql => stat.execute(sql) }
  	  if(this.nodeInfo.sType != SourceType.HIVE) conn.commit()
      true
    }catch{
      case e:Exception => 
        e.printStackTrace()
        if(conn != null && this.nodeInfo.sType != SourceType.HIVE) {
          conn.rollback()
          errorLog("进行回滚")
        }
        errorLog(e.getMessage)
        false
    }finally{
      if(stat != null) stat.close()
      if(conn != null) conn.close()
    }
  }
}