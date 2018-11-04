package com.kent.pub.db

import com.kent.pub.Event._
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import java.sql.PreparedStatement
import java.sql.ResultSet

object MysqlOpera {
  /**
   * 获取数据库连接
   */
  def getConnection(dbLink: DBLink): Connection = {
    	Class.forName("com.mysql.jdbc.Driver")
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
      conn.setAutoCommit(false)
    	stat = conn.createStatement()
      val results = sqls.map { stat.execute(_) }.toList
      conn.commit()
    } catch {
      case e: Exception => conn.rollback();throw e
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
   * 批量执行同一个sql
   */
  def executeBatch(sql: String,rows: List[List[String]])(implicit conn: Connection) = {
    var isTransation = false
    val pstat: PreparedStatement = null
    try{
      if(conn.getAutoCommit){
        isTransation = true
        conn.setAutoCommit(false)
      }
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
          throw e
        }
    }finally{
      if(pstat != null) pstat.close()
      if(isTransation) conn.setAutoCommit(true)
    }
  }
}