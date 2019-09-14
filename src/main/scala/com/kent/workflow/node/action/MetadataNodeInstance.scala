package com.kent.workflow.node.action

import com.kent.workflow.node.NodeInstance

import scala.sys.process.ProcessLogger
import com.kent.main.Worker
import com.kent.pub.Event._

import scala.sys.process._
import java.util.Date
import java.io.PrintWriter
import java.io.File

import com.kent.util.{FileUtil, ParamHandler, Util}
import com.kent.daemon.LogRecorder.LogType
import com.kent.daemon.LogRecorder.LogType._
import akka.pattern.{ask, pipe}

import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout
import com.kent.daemon.LogRecorder

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.Await
import com.kent.pub.db.MysqlOperator
import com.kent.pub.db.DBLink.DatabaseType._
import com.kent.pub.db.DBLink
import com.kent.workflow.node.action.MetadataNode.SelfColumn
import com.kent.pub.db.OracleOperator
import com.kent.pub.db.HiveOperator

class MetadataNodeInstance(override val nodeInfo: MetadataNode) extends ActionNodeInstance(nodeInfo)  {
  implicit val timeout = Timeout(60 seconds)
  
  override def execute(): Boolean = {
    //获取dblink
    val dblOptF = this.actionActor.getDBLink(nodeInfo.dbLinkName)
    val dblOpt = Await.result(dblOptF, 30 seconds)
    if(dblOpt.isEmpty) throw new Exception(s"[db-link:${nodeInfo.dbLinkName}]未配置")
    val dbLink = dblOpt.get
    //获取工作流中的依赖任务(填充表说明)
    val sInfoF = this.actionActor.getInstanceShortInfo()
    val sInfo = Await.result(sInfoF, 30 seconds)
    if(nodeInfo.tableCommentOpt.isEmpty && sInfo.desc != null && sInfo.desc.trim() != ""){
      nodeInfo.tableCommentOpt = Some(sInfo.desc)
    }
    
    //获取目标表的字段信息
    val columnsOpt = getTableColumns(nodeInfo.table, dbLink)
    if(columnsOpt.isEmpty) throw new Exception(s"表${nodeInfo.table}不存在")
    val columns = columnsOpt.get
    
    //获取源表的字段信息，并去重
    val fromColumns = nodeInfo.fromTables.flatMap{ x =>
      val colOpt = getTableColumns(x, dbLink)
      if(colOpt.isEmpty) throw new Exception(s"from属性中源表${x}不存在")
      colOpt.get.filterNot(y => y.comment == null || y.comment.trim() == "").toList
    }.map{ x => (x.name, x) }.toMap
    
    //用目标表字段的注释填充源表的字段集合
    columns.foreach{ x =>
      val satifiedCol = fromColumns.get(x.name)
      x.comment = if(satifiedCol.isDefined) satifiedCol.get.comment else x.comment
    }
    //用配置的字段覆盖目标表字段
    nodeInfo.colCommentMap.foreach{ case(colName, cmmt) =>
      val tmpColOpt = columns.find(_.name == colName)
      if(tmpColOpt.isDefined) {
        tmpColOpt.get.comment =  cmmt
      }else{
        throw new Exception(s"目标表中不存在配置的字段：${colName}")
      }
    }
    //更新表
    val notConfigCols = columns.filter(x => x.comment == null || x.comment.trim() == "")
    if(notConfigCols.size > 0){
      val tmp = notConfigCols.map(x => x.name).mkString(",")
      throw new Exception(s"目标表中的字段未配置注释：${tmp}")
    }
    setTableComment(nodeInfo.table, dbLink, columns, nodeInfo.tableCommentOpt)
    
    true
  }

  def kill(): Boolean = {
    ???
  }
  /**
   * mysql表字段提取
   */
  private def getTableColumns(table: String,dbLink: DBLink): Option[List[SelfColumn]] = {
    var db: String = null
    var tableName: String = null
    val arr = table.split("\\.")
    if(arr.size == 1){
      tableName = arr(0)
    }else{
      db = arr(0)
      tableName = arr(1)
    }
    
    dbLink.dbType match {
      case HIVE => 
        var sql = s"desc ${table}"
        val columns = HiveOperator.query(sql, dbLink, rs => {
           var cols = List[SelfColumn]()
          while(rs.next()){
            val col = SelfColumn(rs.getString(1), rs.getString(2), rs.getString(3))
            	cols = cols :+ col
          }
          if(cols.size == 0) null else cols
        })
        columns
      case MYSQL =>
        var sql = s"select column_name,column_type,COLUMN_COMMENT from information_schema.COLUMNS where TABLE_NAME='${tableName}'"
        if(db != null) sql = sql + s" and TABLE_SCHEMA = '${db}'"
        val columns = MysqlOperator.query(sql, dbLink, rs => {
          var cols = List[SelfColumn]()
          while(rs.next()){
            val col = SelfColumn(rs.getString(1), rs.getString(2), rs.getString(3))
            	cols = cols :+ col
          }
          if(cols.size == 0) null else cols
        }, null)
        columns
      case ORACLE => 
        var sql = s"select column_name,comments from all_col_comments where table_name='${tableName.toUpperCase()}'"
        if(db != null) sql = sql + s" and owner = '${db.toUpperCase()}'"
        val columns = OracleOperator.query(sql, dbLink, rs => {
          var cols = List[SelfColumn]()
          while(rs.next()){
            val cmmt = if(rs.getString(2) == null) "" else rs.getString(2).toLowerCase()
            val col = SelfColumn(rs.getString(1).toLowerCase(), null, cmmt)
            	cols = cols :+ col
          }
          if(cols.size == 0) null else cols
        })
        columns
    }
  }
  private def setTableComment(table: String, dbLink: DBLink, cols: List[SelfColumn], tableCommentOpt: Option[String]) = {
    dbLink.dbType match {
      case HIVE => 
         var sqls = cols.map{ col =>
          s"alter table ${table} change column ${col.name} ${col.name} ${col.colType} comment '${col.comment}'"
        }.toList
        if(tableCommentOpt.isDefined){
          	val otherSql = s"ALTER TABLE ${table} SET TBLPROPERTIES('comment' = '${tableCommentOpt.get}')"
          	sqls = sqls :+ otherSql
        }
        HiveOperator.executeSqls(sqls, dbLink)
         
      case MYSQL =>
        var sqls = cols.map{ col =>
          s"alter table ${table} modify column ${col.name} ${col.colType} comment '${col.comment}'"
        }.toList
        if(tableCommentOpt.isDefined){
          	val otherSql = s"alter table ${table} comment '${tableCommentOpt.get}'"
          	sqls = sqls :+ otherSql
        }
        MysqlOperator.executeSqls(sqls,dbLink, null)
      case ORACLE => 
        var sqls = cols.map{ col =>
          s"comment on column ${table}.${col.name} is '${col.comment}'"
        }.toList
        if(tableCommentOpt.isDefined){
          	val otherSql = s"comment on table ${table} is '${tableCommentOpt.get}'"
          	sqls = sqls :+ otherSql
        }
        OracleOperator.executeSqls(sqls, dbLink)
    }
    
    
    
    
    
    
  }
  
}