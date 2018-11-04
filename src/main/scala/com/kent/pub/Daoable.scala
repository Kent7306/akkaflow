package com.kent.pub

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import scala.util.Try
import scala.collection.JavaConverters._
import java.sql.DriverManager
import org.apache.hive.jdbc.HiveStatement
import java.sql.PreparedStatement

/**
 * 数据操作特质
 */
trait Daoable {
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
        if(isTransation) {
          conn.rollback()
        }
        throw e
    } finally{
      if(stat != null) stat.close()
      if(isTransation) conn.setAutoCommit(true)
    }
    true
  }
}
  