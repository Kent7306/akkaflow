package com.kent.pub.db

import java.sql._

/**
  * JDBC操作抽象类
  */
abstract class JdbcOpera {
  /**
    * 获取数据库连接
    * @param dbLink
    * @return Connection
    */
  def getConnection(dbLink: DBLink): Connection

  /**
    * 得到数据库，或表前缀
    * @param dbLink
    * @return String
    */
  def getDBName(dbLink: DBLink): String

  /**
    * 执行单条SQL
    * @param sql: 执行的SQL
    * @param dbLink: 数据库连接
    * @param stateRecorder: 给外界记录conn及statement，一般用来取消执行
    * @param infoLogHandler: info级别的日志记录处理
    * @param errorLogHandler: error级别的日志记录处理
    * @return Boolean
    */
  def executeSql(sql: String, dbLink: DBLink,
                 stateRecorder:(Connection, Statement) => Unit,
                  infoLogHandler:String => Unit,
                  errorLogHandler: String => Unit): Boolean = {
    this.executeSqls(List(sql), dbLink, stateRecorder,infoLogHandler, errorLogHandler).head
  }

  /**
    * 执行单条SQL
    * @param sql: 执行的SQL
    * @param dbLink: 数据库连接
    * @param stateRecorder: 给外界记录conn及statement，一般用来取消执行
    * @return Boolean
    */
  def executeSql(sql: String, dbLink: DBLink, stateRecorder:(Connection, Statement) => Unit): Boolean = {
    this.executeSql(sql, dbLink, stateRecorder, null, null)
  }
  /**
    * 执行单条SQL
    * @param sql: 执行的SQL
    * @param dbLink: 数据库连接
    * @return Boolean
    */
  def executeSql(sql: String, dbLink: DBLink): Boolean = {
    this.executeSql(sql, dbLink, null, null, null)
  }
  /**
    * 执行多条SQL
    * @param sqls: 执行的SQL集合
    * @param dbLink: 数据库连接
    * @param stateRecorder: 给外界记录conn及statement，一般用来取消执行
    * @param infoLogHandler: info级别的日志记录处理
    * @param errorLogHandler: error级别的日志记录处理
    * @return List[Boolean]
    */
  def executeSqls(sqls: List[String], dbLink: DBLink,
                  stateRecorder:(Connection, Statement) => Unit,
                  infoLogHandler:String => Unit,
                  errorLogHandler: String => Unit): List[Boolean]
  /**
    * 执行多条SQL
    * @param sqls: 执行的SQL集合
    * @param dbLink: 数据库连接
    * @param stateRecorder: 给外界记录conn及statement，一般用来取消执行
    * @return List[Boolean]
    */
  def executeSqls(sqls: List[String], dbLink: DBLink, stateRecorder:(Connection, Statement) => Unit): List[Boolean] = {
    this.executeSqls(sqls, dbLink, stateRecorder, null, null)
  }
  /**
    * 执行多条SQL
    * @param sqls: 执行的SQL集合
    * @param dbLink: 数据库连接
    * @return List[Boolean]
    */
  def executeSqls(sqls: List[String], dbLink: DBLink): List[Boolean] = {
    this.executeSqls(sqls, dbLink, null, null, null)
  }

  /**
    * 查询SQL
    * @param sql SQL
    * @param dbLink 数据库连接
    * @param rsHandler rs处理器，处理每条记录
    * @param stateRecorder 给外界记录conn及statement，一般用来取消执行
    * @param infoLogHandler info级别的日志记录处理
    * @param errorLogHandler error级别的日志记录处理
    * @tparam A 查询组装返回的类型
    * @return Option[A]
    */
  def querySql[A](sql: String, dbLink: DBLink, rsHandler:(ResultSet) => A,
                  stateRecorder:(Connection, Statement) => Unit,
                  infoLogHandler:String => Unit,
                  errorLogHandler: String => Unit): Option[A]
  /**
    * 查询SQL
    * @param sql SQL
    * @param dbLink 数据库连接
    * @param rsHandler rs处理器，处理每条记录
    * @param stateRecorder 给外界记录conn及statement，一般用来取消执行
    * @tparam A 查询组装返回的类型
    * @return Option[A]
    */
  def querySql[A](sql: String, dbLink: DBLink, rsHandler:(ResultSet) => A,
                  stateRecorder:(Connection, Statement) => Unit): Option[A] = {
    this.querySql(sql, dbLink, rsHandler, stateRecorder, null, null)
  }

  /** 查询SQL
    * @param sql SQL
    * @param dbLink 数据库连接
    * @param rsHandler rs处理器，处理每条记录
    * @tparam A 查询组装返回的类型
    * @return Option[A]
    */
  def querySql[A](sql: String, dbLink: DBLink, rsHandler:(ResultSet) => A): Option[A] = {
    this.querySql(sql, dbLink, rsHandler, null, null, null)
  }
}
