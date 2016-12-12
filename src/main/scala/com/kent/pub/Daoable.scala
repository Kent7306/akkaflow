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
   // println(sql)
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
	 // println(sql)
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
  /**
   * 设置该对象的content字段
   */
  def setContent(contentStr: String) = {}
  /**
   * 获取对象的content内容
   */
  def getContent(): String = null
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