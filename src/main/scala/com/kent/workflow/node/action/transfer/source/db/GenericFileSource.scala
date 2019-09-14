package com.kent.workflow.node.action.transfer.source.db

import java.io.BufferedReader

import com.kent.pub.db.Column
import com.kent.pub.db.Column.DataType
import com.kent.pub.io.{FileLink, FileSystemOperator}
import com.kent.workflow.node.action.transfer.source.Source

/**
  * @author kent
  * @date 2019-08-26
  * @desc
  *
  **/
abstract class GenericFileSource(fl: FileLink, delimited: String, path: String) extends Source {

  var br: BufferedReader = _
  var operator: FileSystemOperator = _

  lazy val firstLine: String = {
    if(!operator.isExists(path)) throw new Exception(s"文件${path}不存在")
    val brTmp = operator.getBufferedReader(path)
    val firstLine = brTmp.readLine()
    brTmp.close()
    firstLine
  }
  /**
    * 初始化
    */
  override def init(): Unit = {
    operator = fl.getOperator()
    br = operator.getBufferedReader(path)
  }

  override def getColNums: Option[List[Column]] = {
    if(firstLine == null) {
      None
    } else {
      val list = firstLine.split(this.delimited).zipWithIndex.map{
        case (_,idx) => Column(s"col_$idx", DataType.STRING, 256, 0)
      }.toList
      Some(list)
    }
  }

  /**
    * 获取一批次记录集合
    */
  override def fillRowBuffer(): List[List[String]] = {
    val rowsBuffer = scala.collection.mutable.ArrayBuffer[List[String]]()
    var cnt = 0
    var line = ""
    while (line != null && cnt < ROW_MAX_SIZE)
    {
      line = br.readLine()
      if(line == null){
        isEnd = true
      }else{
        cnt += 1
        val row = line.split(delimited,-1).map { x => if(x == "" || x.toLowerCase() == "null") null else x }.toList
        rowsBuffer.append(row)
      }
    }
    rowsBuffer.toList
  }

  /**
    * 结束
    */
  override def finish(): Unit = {
    if(br != null) br.close()
    if(operator != null) operator.close()
  }
}
