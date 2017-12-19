package com.kent.pub

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

/**
 * 数据操作特质
 */
trait Daoable[A] {
  /**
   * 查询sql
   */
  def querySql[A](sql: String, f:(ResultSet) => A)(implicit conn: Connection): Option[A] = {
    var stat:Statement = null
    try{
    	stat = conn.createStatement()
    	val rs = stat.executeQuery(sql)
    	val obj = f(rs)
    	if(obj != null) Some(obj) else None
    }catch{
      case e:Exception => e.printStackTrace()
      None
    }finally{
      if(stat != null) stat.close()
    }
  }
  /**
   * 执行sql
   */
  def executeSql(sql: String)(implicit conn: Connection): Boolean = executeSql(List(sql))
  /**
   * 批量执行sql，如果有事务，则嵌套在里面，出错则抛出异常；否则，自己起事务，异常自己处理
   */
  def executeSql(sqls: List[String])(implicit conn: Connection):Boolean = {
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
        }
        else{
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
   * 保存或更新对象
   */
  def save(implicit conn: Connection): Boolean
  /**
   * 删除对象及相关联系对象
   */
  def delete(implicit conn: Connection): Boolean
  /**
   * 获取对象
   */
  def getEntity(implicit conn: Connection): Option[A]
}