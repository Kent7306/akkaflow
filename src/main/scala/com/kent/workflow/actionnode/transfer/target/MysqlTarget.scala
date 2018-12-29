package com.kent.workflow.actionnode.transfer.target

import java.sql.Connection
import com.kent.pub.Event._
import com.kent.pub.db.MysqlOpera
import com.kent.workflow.actionnode.transfer.source.Source.Column
import com.kent.workflow.actionnode.transfer.source.Source.DataType._
import com.kent.pub.db.DBLink

class MysqlTarget(isPreTruncate: Boolean, dbLink: DBLink, 
                 table: String, preOpera: String,afterOpera: String) extends Target {
  var conn: Connection = _
  var colNum: Int = 0
  def init(): Boolean = {
    conn = MysqlOpera.getConnection(dbLink)
    conn.setAutoCommit(false)
    true
  }

  def getColNum(cols: List[Column]): Option[Int] = {
    createTableIfNotExist(cols)
    if(this.colNum == 0){
      this.colNum = MysqlOpera.querySql(s"select * from ${table} where 1 < 0", dbLink, rs => {
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
      MysqlOpera.executeBatch(insertSql, rows)(conn)
      true
  }

  def preOpera(): Boolean = {
    val clearSql = if(isPreTruncate) List(s"delete from ${table} where 1 = 1") else List()
    val preSqls = if(preOpera != null) preOpera.split(";").map(_.trim()).filter { _ != "" }.toList else List()
    var sqls = clearSql ++ preSqls
    MysqlOpera.executeSqls(dbLink, sqls)
    true
  }

  def afterOpera(): Boolean = {
    val afterSqls = if(afterOpera != null) afterOpera.split(";").map(_.trim()).filter { _ != "" }.toList else List()
    MysqlOpera.executeSqls(dbLink, afterSqls)
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
      val db = table.split("\\.")(0)
      val tableName = table.split("\\.")(1)
         s"""
           select count(1) cnt from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA='${db}' and TABLE_NAME='${tableName}' 
          """
    }else{
      s"select count(1) cnt from INFORMATION_SCHEMA.TABLES where TABLE_NAME='${table}'"
    }
    
    val exitsNumOpt = MysqlOpera.querySql(exitsSql, dbLink, rs => {
      rs.next()
      rs.getInt(1)
    })
    
    if(exitsNumOpt.get == 0){
      infoLog("目标表不存在，自动创建")
      val colStr = cols.map { col => 
        col.columnType match {
          case STRING => s"${col.columnName} varchar(${col.dataLength})"
          case NUMBER => 
            if(col.precision <= 0) s"${col.columnName} bigint"
            else s"${col.columnName} double"
        }
      }.mkString(",")
      val createSql = s"create table if not exists ${table}(${colStr})"
      infoLog("执行建表语句: "+createSql)
      MysqlOpera.executeSqls(dbLink, List(createSql))
    }else{
      infoLog("目标表存在")
    }
  }
}