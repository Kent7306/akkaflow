package com.kent.pub.db

import com.kent.pub.Event._
import java.sql._
import org.apache.hive.jdbc.HiveStatement
import scala.collection.JavaConverters._

object HiveOpera {
  /**
   * 获取数据库连接
   */
  def getConnection(dbLink: DBLink): Connection = {
    	Class.forName("org.apache.hive.jdbc.HiveDriver")
    	DriverManager.getConnection(dbLink.jdbcUrl, dbLink.username, dbLink.password) 
  }
  /**
   * 执行多条sql语句
   */
  def executeSqls(dbLink: DBLink, sqls: List[String]) = {
    var conn: Connection = null
    var stat: Statement = null
    try {
      conn = getConnection(dbLink)
    	stat = conn.createStatement()
      val results = sqls.map { stat.execute(_) }.toList
    } catch {
      case e: Exception => throw e
    } finally{
      if(stat != null) stat.close()
      if(conn != null) conn.close()
    }
  }
  /**
   * 查询sql(特供actionnode使用)
   */
  def querySql[A](sql: String, dbLink: DBLink, f:(ResultSet) => A): Option[A] = {
    var conn: Connection = null
    var stat: Statement = null
    var rs: ResultSet = null
    try {
    	conn = this.getConnection(dbLink)
      stat = conn.createStatement()
    	val rs = stat.executeQuery(sql)
    	val obj = f(rs)
    	if(obj != null) Some(obj) else None
    } catch{
      case e:Exception => throw e
    }finally{
      if(rs != null) rs.close()
      if(stat != null) stat.close()
      if(conn != null) conn.close()
    }
  }
  
  /**
   * 执行多条sqls(特供actionnode使用), callBack函数主要是用来停止该语句执行
   */
  def executeSqls(sqls: List[String], dbLink: DBLink,callBack:(Connection, Statement) => Unit, 
       infoLogHandle:String => Unit, errorLogHandle: String => Unit) = {
    var conn: Connection = null
    var stat: Statement = null 
    try{
      conn = this.getConnection(dbLink)
  	  stat = conn.createStatement()
  	  callBack(conn,stat)
  	  //打印hive日志
	    val logThread = new Thread(new Runnable() {
  			def run() {
  				val hivestat = stat.asInstanceOf[HiveStatement]
  				while (hivestat.hasMoreLogs())  {  
            hivestat.getQueryLog().asScala.map{ x => infoLogHandle(x)}
            Thread.sleep(500)  
          } 
  			}
  		});  
      logThread.setDaemon(true);  
      logThread.start();  
      //执行
    	val results = sqls.map { sql => stat.execute(sql) }
    }catch{
      case e:Exception => 
        errorLogHandle(e.getMessage)
        throw e
        false
    }finally{
      if(stat != null) stat.close()
      if(conn != null) conn.close()
    }
  }
}