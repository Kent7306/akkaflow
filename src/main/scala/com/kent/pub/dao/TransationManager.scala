package com.kent.pub.dao

import java.sql.Connection

/**
  * @author kent
  * @date 2019-06-29
  * @desc 简单的事务管理器，事务层嵌可以合并
  *
  **/
trait TransationManager {
  /**
    * 事务嵌套执行
    * @param code
    * @tparam A
    * @return
    */
  def transaction[A](code: => A): A = {
    val conn =  ThreadConnector.getConnection()
    val isOuter = if (conn.getAutoCommit){
      conn.setAutoCommit(false)
      true
    } else {
      false
    }
    try{
      code
    } catch {
      case e: Exception =>
        if(isOuter) conn.rollback()
        throw e
    } finally {
      if (isOuter){
        conn.setAutoCommit(true)
        conn.setReadOnly(false)
        ThreadConnector.removeConnection()
      }
    }
  }

}
