package com.kent.pub.db

import java.sql.{Connection, ResultSet, Statement}

import com.kent.pub.db.DBLink.DatabaseType.{DatabaseType, HIVE, MYSQL, ORACLE}
import com.kent.pub.DeepCloneable
import com.kent.pub.Event.Result

import scala.util.Try

/**
  * 数据库连接
  */
class DBLink() extends DeepCloneable[DBLink]{
  var dbType: DatabaseType = _
  var name: String = _
  var jdbcUrl: String = _
  var username: String = _
  var password: String = _
  var description: String = _

  /**
    * 获取当前连接串的数据库或shcema
    * @return
    */
  def getDatabase: String = getJdbcOpera().getDBName(this)

  /**
    * 执行多条sql
    * @param sqls
    * @param stateRecorder
    * @param infoLogHandler
    * @param errorLogHandler
    * @return
    */
  def executeSqls(sqls: List[String],
                  stateRecorder: (Connection, Statement) => Unit,
                  infoLogHandler: String => Unit,
                  errorLogHandler: String => Unit): List[Boolean] = {
    val jo = getJdbcOpera()
    jo.executeSqls(sqls, this, stateRecorder, infoLogHandler, errorLogHandler)
  }

  /**
    * 简单查询指定SQL
    * @param sql
    * @param rsHandler
    * @tparam A
    * @return
    */
  def shortQuery[A](sql: String, rsHandler:ResultSet => A): Option[A] = {
    val jo = getJdbcOpera()
    jo.querySql[A](sql, this, rsHandler)
  }
  /**
    * 判断是否可用连通
    * @return
    */
  def checkIfThrough(): Result ={
    var conn: Connection = null;
    val result = Try {
      conn = getJdbcOpera().getConnection(this)
      Result(true, "测试连通", None)
    }.recover{
      case e: Exception => Result(false, e.getMessage, None)
    }.get
    if (conn != null) conn.close()
    result
  }

  /**
    * 获取jdbc操作器
    * @return
    */
  private def getJdbcOpera(): JdbcOpera = {
    dbType match {
      case HIVE => HiveOpera
      case ORACLE => OracleOpera
      case MYSQL => MysqlOpera
      case _ => ???
    }
  }
}

object DBLink{
  def apply(dbType: DatabaseType, name: String, jdbcUrl: String, username: String, password: String, description: String): DBLink = {
    val dbLink = new DBLink()
    dbLink.dbType = dbType
    dbLink.name = name
    dbLink.jdbcUrl = jdbcUrl
    dbLink.username = username
    dbLink.password = password
    dbLink.description = description
    dbLink
  }
  
  object DatabaseType extends Enumeration {
    type DatabaseType = Value
    val HIVE, ORACLE, MYSQL = Value
  }
}