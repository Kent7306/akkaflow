package com.kent.workflow.actionnode.transfer.source

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.File

  /**
   * 读取本机文件类
   */
class LocalFileSource(delimited: String, path: String) extends Source {
  var br: BufferedReader = null
  var read: InputStreamReader = null
  def init(): Boolean = {
    val read = new InputStreamReader(new FileInputStream(new File(path)))
    br = new BufferedReader(read)
    true
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
    ???
  }
}