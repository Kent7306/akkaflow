package com.kent.workflow.actionnode.transfer.source

import scala.util.Try
import scala.util.Try
import com.kent.workflow.actionnode.transfer.source.Source.Column

trait Source {
  //单次导入记录数最大值
  val ROW_MAX_SIZE:Int = 10000
  //是否结束
  var isEnd = false
  //列数
  var colNum: Int = _
  /**
   * 获取数据源字段数
   */
  def getColNum:Option[Int]
  /**
   * 获取数据源字段类型
   */
  def getColNums: Option[List[Column]]
  /**
   * 获取记录集合
   */
  def fillRowBuffer():List[List[String]]
  /**
   * 初始化
   */
  def init(): Boolean
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
  case class GetColNum()
  case class GetRows()
  case class Rows(rows: Try[List[List[String]]])
  case class End(isSuccess: Boolean)
  case class ColNum(colnum: Try[Option[Int]])
  //
  case class ExecuteResult(isSuccess: Boolean, data: Any)
}