package com.kent.workflow.actionnode.transfer.target

import com.kent.workflow.node.ActionNodeInstance
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream
import java.io.BufferedWriter
import java.net.URI
import org.apache.hadoop.fs.Path
import java.io.OutputStreamWriter
import scala.sys.process._
import com.kent.util.FileUtil
import com.kent.workflow.actionnode.transfer.source.Source

class HdfsTarget(isPreDel: Boolean, delimited: String, path: String,   
      preCmd: String, afterCmd: String, actionInstance: ActionNodeInstance) extends Target {
  var process: Process = null
  
  val conf = new Configuration()
  var fs: FileSystem = null
  var out: FSDataOutputStream = null
  var bw: BufferedWriter = null 
  def init(): Boolean = {
		val (dir, baseName) = FileUtil.getDirAndBaseName(path)
    fs = FileSystem.get(URI.create(dir), conf)
    val out = fs.create(new Path(path));      
    bw = new BufferedWriter(new OutputStreamWriter(out))
    true
  }

  def preOpera(): Boolean = {
    if(preCmd != null){
        actionInstance.executeScript(preCmd, None)(pro => {this.process = pro })
      }else true
  }

  def persist(rows: List[List[String]]): Boolean = {
    val lines = rows.map { x => x.mkString(delimited) }.toList
    totalRowNum += lines.size
    lines.foreach { x => bw.write(x+"\n") }
    bw.flush()
    true
  }

  def afterOpera(): Boolean = {
    if(afterCmd != null){
        actionInstance.executeScript(afterCmd, None)(pro => { this.process = pro })
    }else true
  }

  def finish(isSuccessed: Boolean): Unit = {
    if(bw != null) bw.close()
    if(fs != null) fs.close()
    if(process != null) process.destroy()
  }

  def getColNum(cols: List[Source.Column]): Option[Int] = None
}