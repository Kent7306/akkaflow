package com.kent.pub.dao

import java.sql.Connection

import com.alibaba.druid.pool.DruidDataSource
import javax.sql.DataSource

/**
  * @author kent
  * @date 2019-06-29
  * @desc 本地线程数据库连接对象
  *
  **/
object ThreadConnector {
  private var dataSource:DruidDataSource = _
  private val threadLocalConnection = new ThreadLocal[Connection]()

  def setDataSource(dataSource: DruidDataSource): Unit = {
    this.dataSource = dataSource
  }

  def getDataSource(): DruidDataSource = {
    this.dataSource
  }

  /**
    * 获取本地线程数据库连接对象
 *
    * @return
    */
  def getConnection(): Connection = {
    var conn = threadLocalConnection.get()
    if (conn == null){
      conn = this.dataSource.getConnection
      threadLocalConnection.set(conn)
    }
    conn
  }

  /**
    * 移除本地线程数据库连接对象
    */
  def removeConnection() = {
    val conn = getConnection()
    conn.close()
    threadLocalConnection.remove()
  }

  /**
    * 关闭datasource
    */
  def closeDataSource() = {
    synchronized{
      if (dataSource != null){
        dataSource match {
          case d: DruidDataSource => d.close()
          case _ =>
        }
        dataSource = null
      }
    }
  }
}
