package com.kent.workflow.actionnode.transfer.source

import java.sql.Connection
import com.kent.pub.Event.DBLink
import com.kent.pub.Daoable
import java.sql.PreparedStatement
import java.sql.ResultSet

class DBSource(dbLink: DBLink,  tableSql: String) extends Source with Daoable[DBSource] {
  var conn: Connection = null
  var stat: PreparedStatement = null
  var rs: ResultSet = null
  
  def init(): Boolean = {
    conn = this.getConnection(dbLink)
    true
  }
  
  def fillRowBuffer(): List[List[String]] = {
      val rowsBuffer = scala.collection.mutable.ArrayBuffer[List[String]]()
      var cnt = 0
      while (rs.next() && cnt < ROW_MAX_SIZE) {
        cnt += 1
        val row = (1 to getColNum.get).map(rs.getString(_)).map { x => if(x != null) x.replaceAll("(\n|\r)+", " ") else x }.toList
        rowsBuffer.append(row)
      }
      //结尾
      if(cnt < ROW_MAX_SIZE){
        isEnd = true
      }
      rowsBuffer.toList
  }

  def finish(): Unit = {
    if(rs != null) rs.close()
    if(stat != null) stat.close()
    if(conn != null) conn.close()
  }

  def getColNum: Option[Int] = {
    if(rs == null) {
        val parseTable = if(tableSql.trim().contains(" ")) s"(${tableSql})" else tableSql
        stat = conn.prepareStatement(s"select * from ${parseTable} AAA_BBB_CCC")
        rs = stat.executeQuery()
        this.colNum = rs.getMetaData.getColumnCount
    }
    Some(this.colNum)
  }
  def delete(implicit conn: Connection): Boolean = ???
  def getEntity(implicit conn: Connection): Option[DBSource] = ???
  def save(implicit conn: Connection): Boolean = ???

  def getColNums: Option[List[Source.Column]] = {
    if(rs == null) {
        val parseTable = if(tableSql.trim().contains(" ")) s"(${tableSql})" else tableSql
        stat = conn.prepareStatement(s"select * from ${parseTable} AAA_BBB_CCC")
        rs = stat.executeQuery()
        val md = rs.getMetaData();
        this.colNum = md.getColumnCount
        (1 to this.colNum).map{idx => 
          md.getColumnName(idx)
          md.getColumnTypeName(idx)
          md.getColumnType(idx)
        }
    }
    Some(this.colNum)
    ???
  }
}