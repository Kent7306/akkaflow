package com.kent.pub.db

import java.sql._

object OracleOpera extends JdbcOpera {
  /**
    * 获取数据库连接
    *
    * @param dbLink
    * @return Connection
    */
  override def getConnection(dbLink: DBLink): Connection = {
    Class.forName("oracle.jdbc.driver.OracleDriver")
    DriverManager.getConnection(dbLink.jdbcUrl, dbLink.username, dbLink.password)
  }

  /**
    * 得到数据库，或表前缀
    *
    * @param dbLink
    * @return String
    */
  override def getDBName(dbLink: DBLink): String = dbLink.name

  /**
    * 执行多条SQL
    *
    * @param sqls            : 执行的SQL集合
    * @param dbLink          : 数据库连接
    * @param stateRecorder   : 给外界记录conn及statement，一般用来取消执行
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
      val results = sqls.map { stat.execute(_) }.toList
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
    * @param stateRecorder   给外界记录conn及statement，一般用来取消执行
    * @param infoLogHandler  info级别的日志记录处理
    * @param errorLogHandler error级别的日志记录处理
    * @tparam A 查询组装返回的类型
    * @return Option[A]
    */
  override def querySql[A](sql: String, dbLink: DBLink,
                           rsHandler: ResultSet => A,
                           stateRecorder: (Connection, Statement) => Unit,
                           infoLogHandler: String => Unit, errorLogHandler: String => Unit): Option[A] = {
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
    */
  def executeBatch(sql: String,rows: List[List[String]])(implicit conn: Connection) = {
    var isTransaction = false
    val pstat: PreparedStatement = null
    try{
      if(conn.getAutoCommit){
        isTransaction = true
        conn.setAutoCommit(false)
      }
      val pstat = conn.prepareStatement(sql)
      rows.foreach { cols =>
        cols.zipWithIndex.foreach{ case(str,idx) => pstat.setString(idx+1, str) }
        pstat.addBatch()
      }
      pstat.executeBatch()
    }catch{
      case e: Exception =>
        if(!isTransaction) {
          throw e
        }else{
          conn.rollback()
          throw e
        }
    }finally{
      if(pstat != null) pstat.close()
      if(isTransaction) conn.setAutoCommit(true)
    }
  }
}