package com.kent.workflow.node.action.transfer.target.io

import java.io.{BufferedWriter, OutputStreamWriter}
import java.security.PrivilegedExceptionAction

import com.kent.main.Worker
import com.kent.pub.db.{Column, DBLink, HiveOperator}
import com.kent.pub.io.{FileLink, HdfsFileSystemOperator}
import com.kent.workflow.node.action.transfer.target.Target
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

class HiveTarget(actionName: String, instanceId: String, isPreTruncate: Boolean, dbLink: DBLink, fl: FileLink,
                 table: String, preOperaStr: String,
                 afterOperaStr: String) extends Target(actionName, instanceId) {
  var bw: BufferedWriter = _
  var delimited = 1.asInstanceOf[Char]
  var operator: HdfsFileSystemOperator = _

  
  def getHdfsTmpPath(): String = {
    s"/tmp/${this.hashCode()}_${this.instanceId}.txt"
  }

  def init(): Boolean = {
    val newFl = FileLink(fl.fsType, fl.name, fl.host, fl.port, dbLink.username, dbLink.password, fl.description)
    operator = HdfsFileSystemOperator(newFl)
    bw = operator.getBufferedWriter(getHdfsTmpPath())
    true
  }

  def persist(rows: List[List[String]]): Boolean = {
    val lines = rows.map { x => x.mkString(delimited.toString) }
    totalRowNum += lines.size
    lines.foreach { x => bw.write(x+"\n") }
    bw.flush()
    true
  }

  def preOpera(): Boolean = {
    val clearSql = if(isPreTruncate) {
      infoLog("准备清空表")
      List(s"truncate table ${table}")
    } else {
      List()
    }
    val preSqls = if(preOperaStr != null) preOperaStr.split(";").map(_.trim()).filter { _ != "" }.toList else List()
    var sqls = clearSql ++ preSqls
    HiveOperator.executeSqls(sqls, dbLink)
    true
  }

  def afterOpera(): Boolean = {
    val afterSqls = if(afterOperaStr != null) afterOperaStr.split(";").map(_.trim()).filter { _ != "" }.toList else List()
    HiveOperator.executeSqls(afterSqls, dbLink)
    true
  }
  def finish(isSuccessed: Boolean): Unit = {
    if (bw != null)bw.close()
    if (operator != null) operator.close()
    if(isSuccessed){
      infoLog(s"准备把文件${getHdfsTmpPath()}导入表中...")
      HiveOperator.executeSqls(List(s"LOAD DATA INPATH '${getHdfsTmpPath()}' INTO TABLE ${table}"), dbLink)
      infoLog(s"文件${getHdfsTmpPath()}导入成功")
    }
  }
  /**
    * 获取目标字段个数
    *
    * @param sourceCols
    * @return
    */
  override def getColsWithSourceCols(sourceCols: List[Column]): Option[List[Column]] = {
    //目标表不存在则创建
    val isTableExists = HiveOperator.isTableExist(table, dbLink)
    if (isTableExists){
      infoLog("目标表存在")
    } else {
      infoLog("目标表不存在，将自动建表")
      HiveOperator.createTable(table, sourceCols, dbLink, createSql => {
        infoLog(createSql)
      })
    }
    val cols = HiveOperator.getColumns(s"select * from ${table} where 1 < 0", dbLink)
    Some(cols)
  }
}