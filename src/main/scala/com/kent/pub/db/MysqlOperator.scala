package com.kent.pub.db

import java.sql._
import java.util.Properties

import com.kent.pub.db.Column.DataType
import com.kent.pub.db.Column.DataType.{NUMBER, STRING}

object MysqlOperator extends JdbcOperator{
  /**
    * 获取数据库连接
    *
    * @param dbLink
    * @return Connection
    */
  override def getConnection(dbLink: DBLink): Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    val pp = new Properties()
    pp.setProperty("user", dbLink.username)
    pp.setProperty("password", dbLink.password)
    pp.setProperty("jdbcUrl", dbLink.jdbcUrl)
    dbLink.properties.foreach{ case (k,v) =>
      pp.setProperty(k,v)
    }
    DriverManager.getConnection(dbLink.jdbcUrl, pp)
  }
  /**
    * 得到数据库，或表前缀
    * @param dbLink
    * @return String
    */
  override def getDBName(dbLink: DBLink): String = {
    val regx = ":\\d+/([^\\?]+)".r
    val matches = regx.findAllIn(dbLink.jdbcUrl)
    if(matches.hasNext) matches.next() else ""
  }
  /**
    * 执行多条SQL
    *
    * @param sqls            : 执行的SQL集合
    * @param dbLink          : 数据库连接
    * @param stateRecorder        : 给外界记录conn及statement，一般用来取消执行
    * @param infoLogHandler  : info级别的日志记录处理
    * @param errorLogHandler : error级别的日志记录处理
    * @return List[Boolean]
    */
  override def executeSqls(sqls: List[String], dbLink: DBLink,
                           stateRecorder: (Connection, Statement) => Unit,
                           infoLogHandler: String => Unit,
                           errorLogHandler: String => Unit): List[Boolean] = {
    var conn: Connection = null
    var stat: Statement = null
    try {
      conn = getConnection(dbLink)
      conn.setAutoCommit(false)
      stat = conn.createStatement()
      stateRecorder(conn, stat)
      val results = sqls.map { stat.execute(_) }
      conn.commit()
      results
    } catch {
      case e: Exception => conn.rollback();throw e
    } finally{
      if(stat != null) stat.close()
      if(conn != null) conn.close()
    }
  }

  /**
    * 查询SQL
    *
    * @param sql             SQL
    * @param dbLink          数据库连接
    * @param rsHandler       rs处理器，处理每条记录
    * @param stateRecorder        给外界记录conn及statement，一般用来取消执行
    * @param infoLogHandler  info级别的日志记录处理
    * @param errorLogHandler error级别的日志记录处理
    * @tparam A 查询组装返回的类型
    * @return Option[A]
    */
  override def query[A](sql: String, dbLink: DBLink,
                           rsHandler: ResultSet => A,
                           stateRecorder: (Connection, Statement) => Unit,
                           infoLogHandler: String => Unit,
                           errorLogHandler: String => Unit): Option[A] = {
    var conn: Connection = null
    var stat: Statement = null
    var rs: ResultSet = null
    try {
      conn = this.getConnection(dbLink)
      stat = conn.createStatement()
      stateRecorder(conn, stat)
      rs = stat.executeQuery(sql)
      val obj = rsHandler(rs)
      Option(obj)
    } catch{
      case e:Exception => throw e
    }finally{
      if(rs != null) rs.close()
      if(stat != null) stat.close()
      if(conn != null) conn.close()
    }
  }

  /**
    * 批量执行同一个sql
    * @param sql
    * @param rowContents
    * @param conn
    * @return
    */
  def executeBatch(sql: String,rowContents: List[List[String]])(implicit conn: Connection) = {
    var isTransation = false
    val pstat: PreparedStatement = null
    try{
      if(conn.getAutoCommit){
        isTransation = true
        conn.setAutoCommit(false)
      }
      val pstat = conn.prepareStatement(sql)
      rowContents.foreach { cols =>
        cols.zipWithIndex.foreach{ case(str,idx) => pstat.setString(idx+1, str) }
        pstat.addBatch()
      }
      pstat.executeBatch()
    }catch{
      case e: Exception =>
        if(!isTransation) {
          throw e
        }else{
          conn.rollback()
          throw e
        }
    }finally{
      if(pstat != null) pstat.close()
      if(isTransation) conn.setAutoCommit(true)
    }
  }

  override def getColumns(sql: String, dbl: DBLink): List[Column] = {
    val cols = query(sql, dbl, rs => {
      val md = rs.getMetaData
      (1 to md.getColumnCount).map{ idx =>
        md.getColumnTypeName(idx) match {
          case x if x == "VARCHAR" || x == "CHAR" => Column(md.getColumnName(idx), DataType.STRING,md.getPrecision(idx), 0)
          case x if x == "DATE" || x == "TIME" || x == "DATETIME" || x == "TIMESTAMP" || x == "YEAR" => Column(md.getColumnName(idx), DataType.STRING, md.getPrecision(idx), 0)
          case x if x.contains("INT") => Column(md.getColumnName(idx), DataType.NUMBER, md.getPrecision(idx), 0)
          case x if x == "DOUBLE" =>  Column(md.getColumnName(idx), DataType.NUMBER, 16, 8)
          case x if x == "FLOAT" =>  Column(md.getColumnName(idx), DataType.NUMBER, 16, 8)
          case x if x == "DECIMAL" => Column(md.getColumnName(idx), DataType.NUMBER, 16, 8)
          case x if x == "BIT" => Column(md.getColumnName(idx), DataType.NUMBER, md.getPrecision(idx), 0)
          case x if x == "VARCHAR2" => Column(md.getColumnName(idx), DataType.STRING,md.getPrecision(idx), 0)
          case x if x == "NUMBER" => Column(md.getColumnName(idx), DataType.NUMBER, md.getPrecision(idx), rs.getMetaData.getScale(idx))
          case other => throw new Exception(s"未配置映射的数据类型: $other")
        }
      }.toList
    })
    cols.get
  }

  /**
    * 创建表
    *
    * @param table
    * @param cols
    */
  override def createTable(table: String, cols: List[Column], dbl: DBLink, createSqlHandler: String => Unit): Unit = {
    val colStr = cols.map { col =>
      col.columnType match {
        case STRING => s"${col.columnName} varchar(${col.dataLength})"
        case NUMBER =>
          if(col.precision <= 0) s"${col.columnName} bigint"
          else s"${col.columnName} double"
      }
    }.mkString(",\n")
    val createSql = s"create table if not exists ${table}(\n${colStr})"
    createSqlHandler(createSql)
    execute(createSql, dbl)


  }

  /**
    * 是否表存在
    *
    * @param table
    * @param dbl
    */
  override def isTableExist(table: String, dbl: DBLink): Boolean = {
    val exitsSql =
      if(table.contains(".")){
        val db = table.split("\\.")(0)
        val tableName = table.split("\\.")(1)
        s"""
           select count(1) cnt from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA='$db' and TABLE_NAME='$tableName'
          """
      }else{
        s"select count(1) cnt from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA='${dbl.getDatabase}' and TABLE_NAME='$table'"
      }
    MysqlOperator.query(exitsSql, dbl, rs => {
      rs.next()
      if(rs.getInt(1) > 0) true else false
    }).get
  }
}