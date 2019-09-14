package com.kent.workflow.node.action.transfer.source

import com.kent.daemon.LogRecorder
import com.kent.daemon.LogRecorder.LogType._
import com.kent.pub.db.Column

abstract class Source {
  //单次导入记录数最大值
  val ROW_MAX_SIZE:Int = 20000
  //是否结束
  var isEnd = false
  //列数
  var colNum: Int = _

  doInit()

  def doInit(): Unit ={
    try {
      init()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new Exception("初始化源数据失败: "+e.getMessage)
    }
  }
  /**
    * 初始化
    */
  def init()
  /**
   * 获取数据源字段类型列表
   */
  def doGetColumns: Option[List[Column]] = {
   val cols = getColNums
   this.colNum = if(cols.isEmpty) 0 else cols.get.size
   cols
  }
  def getColNums: Option[List[Column]]

  def doFillRowBuffer():List[List[String]] = {
    val batch = fillRowBuffer()
    if (batch.size < ROW_MAX_SIZE) this.isEnd = true
    batch
  }
  /**
   * 获取一批次记录集合
   */
  def fillRowBuffer():List[List[String]]
  /**
   * 结束
   */
  def finish()

  //日志
  var actionName: String = _
  var instanceId: String = _
  def infoLog(line: String) = LogRecorder.info(ACTION_NODE_INSTANCE, instanceId, actionName, line)
  def errorLog(line: String) = LogRecorder.error(ACTION_NODE_INSTANCE, instanceId, actionName, line)
  def warnLog(line: String) = LogRecorder.warn(ACTION_NODE_INSTANCE, instanceId, actionName, line)
}

object Source {
  object ConnectType extends Enumeration {
    type ConnectType = Value
    val DB,FILE = Value
  }
  
  //Event
  case class GetColNums()
  case class ColNums(colsOpt: Option[List[Column]])
  case class GetRows()
  case class Rows(rows: List[List[String]])
  case class End(isSuccess: Boolean)
  //
  case class ExecuteResult(isSuccess: Boolean, data: Any)
}