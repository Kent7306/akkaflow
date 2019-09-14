package com.kent.pub.db

import java.sql.{Connection, ResultSet, Statement}

import com.kent.pub.db.DBLink.DatabaseType.{DatabaseType, HIVE, MYSQL, ORACLE}
import com.kent.pub._

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
  var properties: Map[String, String] = Map()


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
    jo.query[A](sql, this, rsHandler)
  }
  /**
    * 判断是否可用连通
    * @return
    */
  def checkIfThrough(): Result ={
    var conn: Connection = null;
    val result = Try {
      conn = getJdbcOpera().getConnection(this)
      SucceedResult("测试连通")
    }.recover{
      case e: Exception => FailedResult(e.getMessage)
    }.get
    if (conn != null) conn.close()
    result
  }

  /**
    * 获取jdbc操作器
    * @return
    */
  def getJdbcOpera(): JdbcOperator = {
    dbType match {
      case HIVE => HiveOperator
      case ORACLE => OracleOperator
      case MYSQL => MysqlOperator
      case _ => ???
    }
  }
}

object DBLink{
  def apply(dbType: DatabaseType, name: String, jdbcUrl: String, username: String, password: String, description: String, properties: Map[String, String]): DBLink = {
    val dbLink = new DBLink()
    dbLink.dbType = dbType
    dbLink.name = name
    dbLink.jdbcUrl = jdbcUrl
    dbLink.username = username
    dbLink.password = password
    dbLink.description = description
    dbLink.properties = properties
    dbLink
  }

  def apply(dbType: DatabaseType, name: String, jdbcUrl: String, username: String, password: String, description: String, propertiesStr: String): DBLink = {
    val pp = if (propertiesStr != null && propertiesStr.trim != "") {
      propertiesStr.split("&").map{ kvStr =>
        val kvs = kvStr.split("=")
        if (kvs.length != 2) throw new Exception(s"${kvStr}的配置出错")
        if (kvs(0).trim == "") throw new Exception(s"${kvStr}的配置出错")
        (kvs(0),kvs(1))
      }.filter{ case (k,v) =>
        v.trim != ""
      }.toMap

    } else {
      Map[String, String]()
    }
    DBLink(dbType, name, jdbcUrl, username, password, description, pp)

  }

  def apply(name: String): DBLink = {
    val dbl = new DBLink()
    dbl.name = name
    dbl
  }
  
  object DatabaseType extends Enumeration {
    type DatabaseType = Value
    val HIVE, ORACLE, MYSQL = Value


  }
}