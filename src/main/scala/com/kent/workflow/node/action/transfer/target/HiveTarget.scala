package com.kent.workflow.node.action.transfer.target

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI

import com.kent.main.Worker
import com.kent.pub.db.{DBLink, HiveOpera}
import com.kent.workflow.node.action.transfer.source.Source.Column
import com.kent.workflow.node.action.transfer.source.Source.DataType._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HiveTarget(isPreTruncate: Boolean, dbLink: DBLink, 
                 table: String, preOpera: String,afterOpera: String) extends Target {
  var colNum: Int = 0
  
  var fs: FileSystem = _
  var bw: BufferedWriter = _
  var delimited = 1.asInstanceOf[Char]
  
  def getHdfsUri = Worker.config.getString("workflow.extra.hdfs-uri")
  
  def hdfsTmpPath = s"/tmp/${this.actionName}_${this.instanceId}.txt"
  
  def init(): Boolean = {
    true
  }

  def getColNum(cols: List[Column]): Option[Int] = {
    createTableIfNotExist(cols)
    if(this.colNum == 0){
      this.colNum = HiveOpera.querySql(s"select * from ${table} where 1 < 0", dbLink, rs => {
    	  rs.getMetaData.getColumnCount
      }).get
    }
    Some(this.colNum)
  }

  def persist(rows: List[List[String]]): Boolean = {
    if(bw == null){
      val conf = new Configuration();
      fs = FileSystem.get(new URI(getHdfsUri), conf);
      val out = fs.create(new Path(hdfsTmpPath));        
      bw = new BufferedWriter(new OutputStreamWriter(out))
    }
    
    val lines = rows.map { x => x.mkString(delimited.toString()) }.toList
    totalRowNum += lines.size
    lines.foreach { x => bw.append(s"${x}\n"); }
    bw.flush()
    true
  }

  def preOpera(): Boolean = {
    val clearSql = if(isPreTruncate) List(s"truncate table ${table}") else List()
    val preSqls = if(preOpera != null) preOpera.split(";").map(_.trim()).filter { _ != "" }.toList else List()
    var sqls = clearSql ++ preSqls
    HiveOpera.executeSqls(sqls, dbLink)
    true
  }

  def afterOpera(): Boolean = {
    val afterSqls = if(afterOpera != null) afterOpera.split(";").map(_.trim()).filter { _ != "" }.toList else List()
    HiveOpera.executeSqls(afterSqls, dbLink)
    true
  }
  def finish(isSuccessed: Boolean): Unit = {
    if(bw != null)bw.close()
    if(fs != null)fs.close()
    if(isSuccessed){
      HiveOpera.executeSqls(List(s"LOAD DATA INPATH '${hdfsTmpPath}' INTO TABLE ${table}"), dbLink)
    }
  }
  
  private def createTableIfNotExist(cols: List[Column]){
    val colStr = cols.map { col => 
      col.columnType match {
        case STRING => s"${col.columnName} string"
        case NUMBER => 
          if(col.precision <= 0) s"${col.columnName} bigint"
          else s"${col.columnName} double"
      }
    }.mkString(",")
    val createSql = s"create table if not exists ${table}(${colStr})"
    infoLog("执行建表语句: "+createSql)
    HiveOpera.executeSqls(List(createSql), dbLink)
  }
}