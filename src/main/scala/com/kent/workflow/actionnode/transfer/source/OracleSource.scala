package com.kent.workflow.actionnode.transfer.source

import java.sql._
import com.kent.pub.Event._
import com.kent.pub.db.OracleOpera
import com.kent.workflow.actionnode.transfer.source.Source.Column
import com.kent.workflow.actionnode.transfer.source.Source.DataType

class OracleSource(dbLink: DBLink,  tableSql: String)  extends Source {
  var conn: Connection = null
  var stat: PreparedStatement = null
  var rs: ResultSet = null
  
  def init(): Unit = {
    conn = OracleOpera.getConnection(dbLink)
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
    stat = conn.prepareStatement(s"select * from ${parseTable} AAA_BBB_CCC")
    rs = stat.executeQuery()
    val md = rs.getMetaData();
    val cols = (1 to md.getColumnCount).map{ idx =>
       md.getColumnTypeName(idx) match {
         case x if(x == "VARCHAR2") => Column(md.getColumnName(idx), DataType.STRING,md.getPrecision(idx), 0)
         case x if(x == "DATE") => Column(md.getColumnName(idx), DataType.STRING, 32, 0)
         case x if(x == "NUMBER") => Column(md.getColumnName(idx), DataType.NUMBER, md.getPrecision(idx), rs.getMetaData.getScale(idx))
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