package com.kent.workflow.actionnode.transfer.source

import org.apache.hadoop.conf.Configuration
import java.io.BufferedReader
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import java.io.InputStreamReader
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path
import com.kent.util.FileUtil

class HdfsSource(delimited: String, path: String) extends Source {
  val conf = new Configuration()
  var fs: FileSystem = null
  var in: FSDataInputStream = null
  var bf: BufferedReader = null 
  def init(): Boolean = {
    fs = FileSystem.get(URI.create(path), conf)
    val (dir, baseName) = FileUtil.getDirAndBaseName(path)
    val hdfsPath = new Path(path)
    if(fs.exists(hdfsPath)){
  	  in = fs.open(hdfsPath)
		  bf=new BufferedReader(new InputStreamReader(in))   	  
    }else{
      throw new Exception("文件不存在")
    }
    true
  }

  def getColNum: Option[Int] = {
    val (dir, baseName) = FileUtil.getDirAndBaseName(path)
    val hdfsPath = new Path(path)
    if(fs.exists(hdfsPath)){
  	  val inTmp = fs.open(hdfsPath)
		  val bfTmp =new BufferedReader(new InputStreamReader(inTmp))
  	  val line = bfTmp.readLine()
  	  bfTmp.close()
  	  inTmp.close()
  	  if(line != null) Some(line.split(delimited,-1).size) else None
  	  
    }else{
      throw new Exception("文件不存在")
    }
  }

  def fillRowBuffer(): List[List[String]] = {
    val rowsBuffer = scala.collection.mutable.ArrayBuffer[List[String]]()
    var cnt = 0
    var line = ""
    while (line != null && cnt < ROW_MAX_SIZE)
    {
        line = bf.readLine()
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

  def finish(): Unit = {
		if(in != null) in.close()
    if(bf != null) bf.close()
    if(fs != null) fs.close()
  }

  def getColNums: Option[List[Source.Column]] = {
    ???
  }
}