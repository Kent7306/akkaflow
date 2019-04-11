package com.kent.workflow.node.action.transfer.source

import com.kent.daemon.LogRecorder

import scala.util.Try
import scala.util.Try
import com.kent.workflow.node.action.transfer.source.Source.Column
import com.kent.daemon.LogRecorder.LogType._

trait Source {
  //单次导入记录数最大值
  val ROW_MAX_SIZE:Int = 10000
  //是否结束
  var isEnd = false
  //列数
  var colNum: Int = _
  
  //日志
  var actionName: String = _
  var instanceId: String = _
  def infoLog(line: String) = LogRecorder.info(ACTION_NODE_INSTANCE, instanceId, actionName, line) 
  def errorLog(line: String) = LogRecorder.error(ACTION_NODE_INSTANCE, instanceId, actionName, line)
  def warnLog(line: String) = LogRecorder.warn(ACTION_NODE_INSTANCE, instanceId, actionName, line)
  
  
  /**
   * 获取数据源字段类型列表
   */
  def getAndSetColNums: Option[List[Column]] = {
   val cols = getColNums
   this.colNum = if(cols.isEmpty) 0 else cols.get.size
   cols
  }
  def getColNums: Option[List[Column]]
  /**
   * 获取记录集合
   */
  def fillRowBuffer():List[List[String]]
  /**
   * 初始化
   */
  def init()
  /**
   * 结束
   */
  def finish()
}

object Source {
  object ConnectType extends Enumeration {
    type ConnectType = Value
    val DB,FILE = Value
  }
  
  object DataType extends Enumeration {
    type DataType = Value
    val STRING, NUMBER  = Value
  }
  import DataType._
  case class Column(columnName: String, columnType: DataType, dataLength: Int, precision: Int)
  
  
  //Event
  case class GetColNums()
  case class ColNums(colsOpt: Option[List[Column]])
  case class GetRows()
  case class Rows(rows: List[List[String]])
  case class End(isSuccess: Boolean)
  //
  case class ExecuteResult(isSuccess: Boolean, data: Any)
}