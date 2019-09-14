package com.kent.pub.dao

import java.sql.{ResultSet, Statement}

/**
  * @author kent
  * @date 2019-06-29
  * @desc
  *
  **/
trait Daoable {

  /**
    * 查询SQL
    * @param sql
    * @param rsHandler
    * @tparam A
    * @return
    */
  def query[A](sql: String, rsHandler: ResultSet => A): Option[A] = {
    val conn = ThreadConnector.getConnection()
    var stat:Statement = null
    var rs:ResultSet = null
    try {
      stat = conn.createStatement()
      rs = stat.executeQuery(sql)
      val obj = rsHandler(rs)
      Option(obj)
    } catch{
      case e: Exception => throw e
    } finally {
      if (rs != null) rs.close()
      if (stat != null) stat.close()
    }
  }

  /**
    * 执行指定sql
    * @param sql
    * @return
    */
  def execute(sql: String): Boolean = {
    val conn = ThreadConnector.getConnection()
    var stat:Statement = null
    try {
      stat = conn.createStatement()
      stat.execute(sql)
    } catch {
      case e: Exception => throw e
    } finally{
      if(stat != null) stat.close()
    }
  }

  /**
    * 执行多条sql
    * @param sqls
    * @return
    */
  def execute(sqls: List[String]): List[Boolean] = {
    val conn = ThreadConnector.getConnection()
    var stat:Statement = null
    try {
      stat = conn.createStatement()
      sqls.map{sql => stat.execute(sql)}
    } catch {
      case e: Exception => throw e
    } finally{
      if(stat != null) stat.close()
    }
  }
}
