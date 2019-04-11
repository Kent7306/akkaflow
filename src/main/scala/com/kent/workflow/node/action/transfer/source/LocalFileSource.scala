package com.kent.workflow.node.action.transfer.source

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.File
import com.kent.workflow.node.action.transfer.source.Source.Column
import com.kent.workflow.node.action.transfer.source.Source.DataType

  /**
   * 读取本机文件类
   */
class LocalFileSource(delimited: String, path: String) extends Source {
  var br: BufferedReader = null
  var read: InputStreamReader = null
  def init() = {
    val read = new InputStreamReader(new FileInputStream(new File(path)))
    br = new BufferedReader(read)
  }
  
  def fillRowBuffer(): List[List[String]] = {
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

  def getColNum: Option[Int] = {
    val read = new InputStreamReader(new FileInputStream(new File(path)))
    val brTmp = new BufferedReader(read)
    val firstLine = brTmp.readLine()
    brTmp.close()
    read.close()
    if(firstLine == null) None else Some(firstLine.split(delimited,-1).size)
  }

  def finish(): Unit = {
    if(br != null) br.close()
    if(read != null) read.close()
  }

  def getColNums: Option[List[Source.Column]] = {
    val read = new InputStreamReader(new FileInputStream(new File(path)))
    val brTmp = new BufferedReader(read)
    val firstLine = brTmp.readLine()
    brTmp.close()
    read.close()
    if(firstLine == null) {
      None 
    } else {
      var i = 0;
      val list = firstLine.split(this.delimited).zipWithIndex.map{
        case (x,idx) => Column(s"col_${idx}", DataType.STRING, 256, 0) 
      }.toList
      Some(list)
    }
  }
}