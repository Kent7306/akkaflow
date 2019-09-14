package com.kent.pub.db

import java.sql._

/**
  * JDBC操作抽象类
  */
abstract class JdbcOperator {
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
  def execute(sql: String, dbLink: DBLink,
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
  def execute(sql: String, dbLink: DBLink, stateRecorder:(Connection, Statement) => Unit): Boolean = {
    this.execute(sql, dbLink, stateRecorder, _ => {}, _ => {})
  }
  /**
    * 执行单条SQL
    * @param sql: 执行的SQL
    * @param dbLink: 数据库连接
    * @return Boolean
    */
  def execute(sql: String, dbLink: DBLink): Boolean = {
    this.execute(sql, dbLink, (_,_) => {}, _ => {}, _ => {})
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
    this.executeSqls(sqls, dbLink, stateRecorder, _ => {}, _ => {})
  }
  /**
    * 执行多条SQL
    * @param sqls: 执行的SQL集合
    * @param dbLink: 数据库连接
    * @return List[Boolean]
    */
  def executeSqls(sqls: List[String], dbLink: DBLink): List[Boolean] = {
    this.executeSqls(sqls, dbLink, (_,_) => {}, _ => {}, _ => {})
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
  def query[A](sql: String, dbLink: DBLink, rsHandler: ResultSet => A,
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
  def query[A](sql: String, dbLink: DBLink, rsHandler:(ResultSet) => A,
                  stateRecorder:(Connection, Statement) => Unit): Option[A] = {
    this.query(sql, dbLink, rsHandler, stateRecorder, _ => {}, _ => {})
  }

  /** 查询SQL
    * @param sql SQL
    * @param dbLink 数据库连接
    * @param rsHandler rs处理器，处理每条记录
    * @tparam A 查询组装返回的类型
    * @return Option[A]
    */
  def query[A](sql: String, dbLink: DBLink, rsHandler:ResultSet => A): Option[A] = {
    this.query[A](sql, dbLink, rsHandler, (_: Connection,_: Statement) => {}, (_: String) => {}, (_: String) => {})
  }

  /**
    * 执行多条sql
    * @param sqls
    * @param conn
    * @return
    */
  def execute(sqls: List[String])(implicit conn: Connection): Boolean = {
    var stat:Statement = null
    try {
      stat = conn.createStatement()
      sqls.map{sql => stat.execute(sql)}
      true
    } catch {
      case e: Exception => throw e
    } finally{
      if(stat != null) stat.close()
    }
  }

  /**
    * 查询
    * @param sql
    * @param rsHandler
    * @param conn
    * @tparam A
    * @return
    */
  def query[A](sql: String, rsHandler: ResultSet => A)(implicit conn: Connection): Option[A] = {
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
    * 获取查询sql的字段集合
    * @param sql
    * @param dbl
    * @return
    */
  def getColumns(sql: String, dbl: DBLink): List[Column]

  /**
    * 创建表
    * @param table
    * @param cols
    */
  def createTable(table: String,cols: List[Column], dbl: DBLink, createSqlHandler: String => Unit)

  /**
    * 是否表存在
    * @param table
    * @param dbl
    */
  def isTableExist(table: String, dbl: DBLink): Boolean


}
