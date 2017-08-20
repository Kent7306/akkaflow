package com.kent.pub

import java.sql.Connection
import java.sql.ResultSet

/**
 * 数据操作特质
 */
trait Daoable[A] {
  /**
   * 查询sql
   */
  def querySql[A](sql: String, f:(ResultSet) => A)(implicit conn: Connection): Option[A] = {
    val stat = conn.createStatement()
    try{
    	val rs = stat.executeQuery(sql)
    	val obj = f(rs)
    	if(obj != null) return Some(obj)
    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      if(stat != null) stat.close()
    }
    None
  }
  /**
   * 执行sql
   */
  def executeSql(sql: String)(implicit conn: Connection): Boolean = {
	  //println(sql)
    val stat = conn.createStatement()
    var result:Boolean = false
    try{
    	result = stat.execute(sql)      
    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      if(stat != null) stat.close()
      result
    }
    result
  }
  def executeSql(sqls: List[String])(implicit conn: Connection):Boolean = {
    try {
      conn.setAutoCommit(false)
    	val stat = conn.createStatement()
      val results = sqls.map { stat.execute(_) }.toList
      conn.commit()
    } catch {
      case e: Exception => 
          conn.rollback()
          e.printStackTrace()
          throw new Exception("执行初始化建表sql失败")
          return false
    }
    conn.setAutoCommit(true)
    true
  }
  /**
   * 根据json来填充对应对象
   */
  def parseJsonStr(contentStr: String) = {}
  /**
   * 组装对象的content内容
   */
  def assembleJsonStr(): String = null
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