package com.kent.workflow.actionnode.transfer.target

import java.sql.Connection
import com.kent.pub.Event._
import com.kent.pub.db.OracleOpera
import com.kent.workflow.actionnode.transfer.source.Source.Column
import com.kent.workflow.actionnode.transfer.source.Source.DataType._
import com.kent.pub.db.DBLink

class OracleTarget(isPreTruncate: Boolean, dbLink: DBLink, 
                 table: String, preOpera: String,afterOpera: String) extends Target {
  var conn: Connection = _
  var colNum: Int = 0
  def init(): Boolean = {
    conn = OracleOpera.getConnection(dbLink)
    conn.setAutoCommit(false)
    true
  }

  def getColNum(cols: List[Column]): Option[Int] = {
    createTableIfNotExist(cols)
    if(this.colNum == 0){
      this.colNum = OracleOpera.querySql(s"select * from ${table} where 1 < 0", dbLink, rs => {
    	  rs.getMetaData.getColumnCount
      }).get
    }
    Some(this.colNum)
  }

  def persist(rows: List[List[String]]): Boolean = {
    val paramLine = (1 to this.colNum).map{x => "?"}.mkString(",")
      val insertSql = s"insert into ${table} values(${paramLine})"
      totalRowNum += rows.size
      if(rows.filter { _.size != this.colNum }.size > 0) throw new Exception(s"存在某（多条）记录的字段个数不等于${this.colNum}")
      OracleOpera.executeBatch(insertSql, rows)(conn)
      true
  }

  def preOpera(): Boolean = {
    val clearSql = if(isPreTruncate) List(s"delete from ${table} where 1 = 1") else List()
    val preSqls = if(preOpera != null) preOpera.split(";").map(_.trim()).filter { _ != "" }.toList else List()
    var sqls = clearSql ++ preSqls
    OracleOpera.executeSqls(dbLink, sqls)
    true
  }

  def afterOpera(): Boolean = {
    val afterSqls = if(afterOpera != null) afterOpera.split(";").map(_.trim()).filter { _ != "" }.toList else List()
    OracleOpera.executeSqls(dbLink, afterSqls)
    true
  }

  def finish(isSuccessed: Boolean): Unit = {
    if(!isSuccessed && conn != null){
      conn.rollback() 
    }else if(conn != null) {
      conn.commit()
    }
    if(conn != null) conn.close()
  }
  
  private def createTableIfNotExist(cols: List[Column]){
    val exitsSql = 
    if(table.contains(".")){
      val user = table.split("\\.")(0)
      val tableName = table.split("\\.")(1)
         s"""select count(1) cnt from all_tables 
                where table_name = '${tableName}' and owner = '${user}'
          """
    }else{
      s"select count(1) cnt from user_tables where table_name = '${table}'"
    }
    
    val exitsNumOpt = OracleOpera.querySql(exitsSql, dbLink, rs => {
      rs.next()
      rs.getInt(1)
    })
    if(exitsNumOpt.get == 0){
      infoLog("目标表不存在，自动创建")
      val colStr = cols.map { col => 
        col.columnType match {
          case STRING => s"${col.columnName} varchar2(${col.dataLength})"
          case NUMBER => 
            if(col.precision <= 0) s"${col.columnName} number(${col.dataLength})"
            else s"${col.columnName} number(${col.dataLength},${col.precision})"
        }
      }.mkString(",")
      val createSql = s"create table ${table}(${colStr})"
      infoLog("执行建表语句: "+createSql)
      OracleOpera.executeSqls(dbLink, List(createSql))
    }else{
      infoLog("目标表存在")
    }
  }
}