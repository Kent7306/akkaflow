package com.kent.pub.db

import com.kent.pub.Event._
import java.sql._

import com.kent.daemon.LineageRecorder.LineageRecord
import com.kent.lineage.HiveLineageInfo
import org.apache.hive.jdbc.HiveStatement

import scala.collection.JavaConverters._
import com.kent.lineage.LineageTable
import com.kent.lineage.LineageTableRef

object HiveOpera extends JdbcOpera {
  /**
    * 获取数据库连接
    *
    * @param dbLink
    * @return Connection
    */
  override def getConnection(dbLink: DBLink): Connection = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    DriverManager.getConnection(dbLink.jdbcUrl, dbLink.username, dbLink.password)
  }
  /**
    * 得到数据库，或表前缀
    *
    * @param dbLink
    * @return String
    */
  override def getDBName(dbLink: DBLink): String = "default"
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
    try{
      conn = this.getConnection(dbLink)
      stat = conn.createStatement()
      if(stateRecorder != null)stateRecorder(conn, stat)
      //打印hive日志
      val logThread = new Thread(() => {
        val hiveStat = stat.asInstanceOf[HiveStatement]
        while (hiveStat != null && hiveStat.hasMoreLogs()) {
          hiveStat.getQueryLog().asScala.map { x => infoLogHandler(x) }
          Thread.sleep(500)
        }
      });
      if (infoLogHandler != null){
        logThread.setDaemon(true)
        logThread.start()
      }
      //执行
      val results = sqls.map { sql => stat.execute(sql) }
      results
    }catch{
      case e:Exception =>
        infoLogHandler(e.getMessage)
        throw e
    }finally{
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
                           infoLogHandler: String => Unit,
                           errorLogHandler: String => Unit): Option[A] = {
    var conn: Connection = null
    var stat: Statement = null
    var rs: ResultSet = null
    try {
      conn = this.getConnection(dbLink)
      stat = conn.createStatement()
      if(stateRecorder != null)stateRecorder(conn, stat)
      //打印hive日志
      val logThread = new Thread(() => {
        val hiveStat = stat.asInstanceOf[HiveStatement]
        while (hiveStat != null && hiveStat.hasMoreLogs()) {
          hiveStat.getQueryLog().asScala.map { x => infoLogHandler(x) }
          Thread.sleep(500)
        }
      });
      if (infoLogHandler != null){
        logThread.setDaemon(true)
        logThread.start()
      }
      val rs = stat.executeQuery(sql)
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
    * 通过sqls得到有血缘关系的集合
    */
  def getLineageRecords(queries: List[String], defaultDb: String): List[LineageRecord] = {
    var useDb = defaultDb
    var lineages = scala.collection.mutable.ListBuffer[SqlLineage]()
    queries.foreach{ sql =>
      val lineage = getSqlLineage(sql, useDb)
      lineage match {
        case l@SqlLineage(_, outputs, None) if outputs.size > 0 => lineages += l
        case SqlLineage(_, _, Some(db)) => useDb = db
        case _ =>
      }
    }
    lineages.flatMap{ case l =>
      l.outputs.map{outTable =>
        val t = LineageTable(outTable)
        val relate = LineageTableRef(outTable, l.inputs.toList)
        LineageRecord(t, relate)
      }
    }.toList
  }
  /**
    * 解析sql，得到该sql的输入输出表格（如果有的话）
    * 如果切换数据库，则switchDbName不为None
    */
  private def getSqlLineage(query: String, defaultDb: String):SqlLineage  = {
    val lep = new HiveLineageInfo()
    lep.getLineageInfo(query.toLowerCase())
    val inputTables = lep.getInputTableList().asScala.map{getTableWithDb(_, defaultDb)}.toSet
    val withTables = lep.getWithTableList().asScala.map{getTableWithDb(_, defaultDb)}.toSet
    val pureInputTables = inputTables -- withTables
    val outputTables = lep.getOutputTableList().asScala.map{getTableWithDb(_, defaultDb)}.toSet
    val switchDbName = if(lep.getSwitchDbName() == null) None else Some(lep.getSwitchDbName())
    SqlLineage(pureInputTables, outputTables, switchDbName)
  }

  private def getTableWithDb(table: String, db: String): String = {
    if(table.contains(".")) table else s"${db}.${table}"
  }

  case class SqlLineage(inputs: Set[String], outputs: Set[String], db: Option[String])



}