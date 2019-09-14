package com.kent.pub.db


import java.sql._
import java.util.Properties

import com.kent.lineage.HiveLineageInfo
import com.kent.pub.db.Column.DataType
import com.kent.pub.db.Column.DataType.{NUMBER, STRING}
import org.apache.hive.jdbc.HiveStatement

import scala.collection.JavaConverters._
import scala.util.Try

object HiveOperator extends JdbcOperator {
  /**
    * 获取数据库连接
    *
    * @param dbLink
    * @return Connection
    */
  override def getConnection(dbLink: DBLink): Connection = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val pp = new Properties()
    pp.setProperty("user", dbLink.username)
    pp.setProperty("password", dbLink.password)
    pp.setProperty("jdbcUrl", dbLink.jdbcUrl)
    dbLink.properties.foreach{ case (k,v) =>
      println(k+"="+v)
      pp.setProperty(k,v)
    }
    DriverManager.getConnection(dbLink.jdbcUrl, pp)
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
    var hiveStat: HiveStatement = null
    try{
      conn = this.getConnection(dbLink)
      stat = conn.createStatement()
      stateRecorder(conn, stat)
      //打印hive日志
      val logThread = new Thread(() => {
        hiveStat =  stat.asInstanceOf[HiveStatement]
        while (hiveStat != null && hiveStat.hasMoreLogs()) {
          try{
            hiveStat.getQueryLog().asScala.foreach { x => infoLogHandler(x) }
          } catch{
            case e: Exception =>
              errorLogHandler(s"（注意）hive线程获取日志时，statement值被修改为null")
              errorLogHandler(e.getMessage)
              e.printStackTrace()
              hiveStat = null
          }
          Thread.sleep(500)
        }
      })
      if (infoLogHandler != null) {
        logThread.setDaemon(true)
        logThread.start()
      }
      //设置参数
      dbLink.properties.foreach{ case (k, v) =>
        stat.execute(s"set $k=$v")
      }

      //执行
      val results = sqls.map { sql => stat.execute(sql) }
      results
    }catch{
      case e:Exception =>
        errorLogHandler(s"（注意）hive线程获取日志时，statement值被修改为null")
        throw e
    }finally{
      if(stat != null) {
        stat.close()
        stat = null
        hiveStat = null
      }
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
  override def query[A](sql: String, dbLink: DBLink,
                           rsHandler: ResultSet => A,
                           stateRecorder: (Connection, Statement) => Unit,
                           infoLogHandler: String => Unit,
                           errorLogHandler: String => Unit): Option[A] = {
    var conn: Connection = null
    var stat: Statement = null
    var hiveStat: HiveStatement = null
    var rs: ResultSet = null
    try {
      conn = this.getConnection(dbLink)
      stat = conn.createStatement()
      stateRecorder(conn, stat)
      //打印hive日志
      val logThread = new Thread(() => {
        hiveStat = stat.asInstanceOf[HiveStatement]
        while (hiveStat != null && hiveStat.hasMoreLogs()) {
          hiveStat.getQueryLog().asScala.map { x => infoLogHandler(x) }
          Thread.sleep(500)
        }
      })
      if (infoLogHandler != null){
        logThread.setDaemon(true)
        logThread.start()
      }

      //设置参数
      dbLink.properties.foreach{ case (k, v) =>
        stat.execute(s"set $k=$v")
      }

      val rs = stat.executeQuery(sql)
      val obj = rsHandler(rs)
      Option(obj)
    } catch{
      case e:Exception => throw e
    }finally{
      if(rs != null) rs.close()
      if(stat != null) {
        stat.close()
        stat = null
        hiveStat = null
      }
      if(conn != null) conn.close()
    }
  }

  /**
    * 通过sqls得到有血缘关系的集合
    */
 /* def getLineageRecords(queries: List[String], defaultDb: String): List[LineageRecord] = {
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
  }*/
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


  override def getColumns(sql: String, dbl: DBLink): List[Column] = {
    val cols = query(sql, dbl, rs => {
      val md = rs.getMetaData
      (1 to md.getColumnCount).map{ idx =>
        val colName = if(md.getColumnName(idx).contains(".")){
          md.getColumnName(idx).split("\\.")(1)
        } else{
          md.getColumnName(idx)
        }
        md.getColumnTypeName(idx).toLowerCase() match {
          case x if x == "string" => Column(colName, DataType.STRING,128, 0)
          case x if x == "date" => Column(colName, DataType.STRING, 32, 0)
          case x if x == "int" => Column(colName, DataType.NUMBER, 10, 0)
          case x if x == "bigint" => Column(colName, DataType.NUMBER, 20, 0)
          case x if x == "bigint" => Column(colName, DataType.NUMBER, 20, 0)
          case x if x.contains("int") => Column(colName, DataType.NUMBER, 6, 0)
          case x if x == "double" || x == "float" => Column(colName, DataType.NUMBER, 16, 8)
          case x if x == "array" => Column(colName, DataType.STRING, 512, 0)
          case other => throw new Exception(s"未配置映射的hive数据类型: $other")
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
        case STRING => s"${col.columnName} string"
        case NUMBER =>
          if(col.precision <= 0) s"${col.columnName} bigint"
          else s"${col.columnName} double"
      }
    }.mkString(",\n")
    val createSql = s"create table if not exists ${table}(\n${colStr}) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'"
    createSqlHandler(createSql)
    HiveOperator.executeSqls(List(createSql), dbl)
  }

  /**
    * 是否表存在
    *
    * @param table
    * @param dbl
    */
  override def isTableExist(table: String, dbl: DBLink): Boolean = {
    Try {
      query(s"desc $table", dbl, rs => {
        return true
      }).get
    }.recover{
      case _: Exception => false
    }.get
  }
}