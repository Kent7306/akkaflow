package com.kent.workflow.node.action.transfer.source

import com.kent.pub.Event._
import java.sql._
import com.kent.workflow.node.action.transfer.source.Source.Column
import com.kent.workflow.node.action.transfer.source.Source.DataType
import org.apache.hive.jdbc.HiveStatement
import scala.collection.JavaConverters._
import com.kent.pub.db.HiveOpera
import com.kent.pub.db.DBLink

class HiveSource(dbLink: DBLink,  tableSql: String) extends Source {
  var conn: Connection = null
  var stat: Statement = null
  var rs: ResultSet = null
  
  def init() = {
    conn = HiveOpera.getConnection(dbLink)
  }

  def fillRowBuffer(): List[List[String]] = {
    val rowsBuffer = scala.collection.mutable.ArrayBuffer[List[String]]()
    var cnt = 0
    while (cnt < ROW_MAX_SIZE && rs.next()) {
      cnt += 1
      val row = (1 to this.colNum).map(rs.getString(_)).map { x => if(x != null) x.replaceAll("(\n|\r)+", " ") else x }.toList
      rowsBuffer.append(row)
    }
    //结尾
    if(cnt < ROW_MAX_SIZE){
      isEnd = true
    }
    rowsBuffer.toList
  }

  def getColNums: Option[List[Source.Column]] = {
    val parseTable = if(tableSql.trim().contains(" ")) s"(${tableSql})" else tableSql
    stat = conn.createStatement()
    
    //打印hive日志
	    val logThread = new Thread(new Runnable() {
  			def run() {
  				val hivestat = stat.asInstanceOf[HiveStatement]
  				while (hivestat.hasMoreLogs())  {  
            hivestat.getQueryLog().asScala.map{ x => infoLog(x)}
            Thread.sleep(500)  
          } 
  			}
  		});  
      logThread.setDaemon(true);  
      logThread.start(); 
    
    
    rs = stat.executeQuery(s"select * from ${parseTable} AAA_BBB_CCC")
    val md = rs.getMetaData();
    val cols = (1 to md.getColumnCount).map{ idx =>
      val colName = if(md.getColumnName(idx).contains(".")){
        md.getColumnName(idx).split("\\.")(1)
      } else{
        md.getColumnName(idx)
      }
      md.getColumnTypeName(idx).toLowerCase() match {
         case x if(x == "string") => Column(colName, DataType.STRING,128, 0)
         case x if(x == "date") => Column(colName, DataType.STRING, 32, 0)
         case x if(x == "int") => Column(colName, DataType.NUMBER, 10, 0)
         case x if(x == "bigint") => Column(colName, DataType.NUMBER, 20, 0)
         case x if(x == "bigint") => Column(colName, DataType.NUMBER, 20, 0)
         case x if(x.contains("int")) => Column(colName, DataType.NUMBER, 6, 0)
         case x if(x == "double" || x == "float") => Column(colName, DataType.NUMBER, 16, 8)
         case other => throw new Exception(s"未配置映射的mysql数据类型: ${other}")
      }
    }.toList
    Some(cols)
 }

  def finish(): Unit = {
    if(rs != null) rs.close()
    if(stat != null) stat.close()
    if(conn != null) conn.close()
  }
}