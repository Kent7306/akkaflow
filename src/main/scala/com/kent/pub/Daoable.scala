package com.kent.pub

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import scala.util.Try
import scala.collection.JavaConverters._
import com.kent.pub.Event.DBLink
import com.kent.workflow.actionnode.DataMonitorNode.DatabaseType._
import java.sql.DriverManager
import org.apache.hive.jdbc.HiveStatement
import java.sql.PreparedStatement

/**
 * 数据操作特质
 */
trait Daoable[A] {
  /**
   * 查询sql
   */
  def querySql[A](sql: String, f:(ResultSet) => A)(implicit conn: Connection): Option[A] = {
    var stat:Statement = null
    var rs:ResultSet = null
    try{
    	stat = conn.createStatement()
    	rs = stat.executeQuery(sql)
    	val obj = f(rs)
    	if(obj != null) Some(obj) else None
    }catch{
      case e:Exception => throw e
    }finally{
    	if(rs != null) rs.close()
      if(stat != null) stat.close()
    }
  }
  /**
   * 执行sql
   */
  def executeSql(sql: String)(implicit conn: Connection): Boolean = executeSqls(List(sql))
  /**
   * 批量执行sql，如果外层有事务，则嵌套在里面，出错则抛出异常；否则，自己起事务，异常自己处理
   */
  def executeSqls(sqls: List[String])(implicit conn: Connection):Boolean = {
    var isTransation = false
    if(conn.getAutoCommit){
      isTransation = true
      conn.setAutoCommit(false)
    }
    var stat:Statement = null
    try {
    	stat = conn.createStatement()
      val results = sqls.map { stat.execute(_) }.toList
      if(isTransation) conn.commit()
    } catch {
      case e: Exception => 
        if(!isTransation) {
          throw e
        }else{
          conn.rollback()
          false
        }
    } finally{
      if(stat != null) stat.close()
      if(isTransation) conn.setAutoCommit(true)
    }
    true
  }
  /**
   * 批量执行同一个sql
   */
  def executeBatch(sql: String,rows: List[List[String]] )(implicit conn: Connection): Boolean = {
    var isTransation = false
    if(conn.getAutoCommit){
      isTransation = true
      conn.setAutoCommit(false)
    }
    val pstat: PreparedStatement = null
    try{
      val pstat = conn.prepareStatement(sql)
      rows.foreach { cols =>
        cols.zipWithIndex.foreach{ case(str,idx) => pstat.setString(idx+1, str) }
        pstat.addBatch()
      }
      pstat.executeBatch()
    }catch{
      case e: Exception =>
        if(!isTransation) {
          throw e
        }else{
          conn.rollback()
          false
        }
    }finally{
      if(pstat != null) pstat.close()
      if(isTransation) conn.setAutoCommit(true)
    }
    true
  }
  /**
   * 根据db-link获取Connection
   */
  def getConnection(dbLink: DBLink): Connection = {
    val driverClsOpt = dbLink.dbType match {
        case MYSQL => Some("com.mysql.jdbc.Driver")
        case ORACLE => Some("oracle.jdbc.driver.OracleDriver")
        case HIVE => Some("org.apache.hive.jdbc.HiveDriver")
        case _ => None
      }
    if(driverClsOpt.isEmpty) throw new Exception("不存在数据类型："+dbLink.dbType)
    else{
    	Class.forName(driverClsOpt.get)
    	//得到连接
    	DriverManager.getConnection(dbLink.jdbcUrl, dbLink.username, dbLink.password) 
    }
  }
  /**
   * 查询sql(特供actionnode使用)
   */
  def querySql[A](sql: String, dbLink: DBLink, f:(ResultSet) => A): Option[A] = {
    implicit val conn = this.getConnection(dbLink)
    try {
      this.querySql(sql, f)
    } catch{
      case e:Exception => throw e
    }finally{
      if(conn != null) conn.close()
    }
  }
  
  /**
   * 执行多条sqls(特供actionnode使用)
   */
  def executeSqls(sqls: List[String], dbLink: DBLink,
       callBack:(Connection, Statement) => Unit, 
       infoLogHandle:String => Unit, 
       errorLogHandle: String => Unit):Boolean = {
    var conn: Connection = null
    var stat: Statement = null 
    try{
      conn = this.getConnection(dbLink)
      conn.setAutoCommit(false)
  	  stat = conn.createStatement()
  	  callBack(conn,stat)
  	  //打印hive日志
  	  if(dbLink.dbType == HIVE){
  	    val logThread = new Thread(new Runnable() {
    			def run() {
    				val hivestat = stat.asInstanceOf[HiveStatement]
    				while (hivestat.hasMoreLogs())  {  
              hivestat.getQueryLog().asScala.map{ x => infoLogHandle(x)}
              Thread.sleep(1000)  
            } 
    			}
    		});  
        logThread.setDaemon(true);  
        logThread.start();  
  	  }
    	val results = sqls.map { sql => stat.execute(sql) }
  	  if(dbLink.dbType != HIVE) conn.commit()
      true
    }catch{
      case e:Exception => 
        if(conn != null &&dbLink.dbType != HIVE) {
          conn.rollback()
          errorLogHandle("进行回滚")
        }
        errorLogHandle(e.getMessage)
        println(e.getMessage)
        false
    }finally{
      if(stat != null) stat.close()
      if(conn != null) conn.close()
    }
  }
  
  
  
/*  *//**
   * 保存或更新对象
   *//*
  def save(implicit conn: Connection): Boolean
  *//**
   * 删除对象及相关联系对象
   *//*
  def delete(implicit conn: Connection): Boolean
  *//**
   * 获取对象
   *//*
  def getEntity(implicit conn: Connection): Option[A]*/
}
  