package com.kent.workflow.actionnode.transfer.target

import com.kent.workflow.node.ActionNodeInstance
import com.kent.util.FileUtil
import scala.sys.process._
import com.kent.workflow.actionnode.transfer.source.Source

class LocalFileTarget(isPreDel: Boolean, delimited: String, path: String,   
        preCmd: String, afterCmd: String, actionInstance: ActionNodeInstance) extends Target {
    var process: Process = null
    
    def init(): Boolean = true

    def persist(rows: List[List[String]]): Boolean = {
      val lines = rows.map { x => x.mkString(delimited) }.toList
      totalRowNum += lines.size
      FileUtil.writeFile(path, lines)(!isPreDel)
      true
    }

    def preOpera(): Boolean = {
      if(preCmd != null){
      actionInstance.executeScript(preCmd, None)(pro => {this.process = pro })
      }else true
    }

    def afterOpera(): Boolean = {
      if(afterCmd != null){
        actionInstance.executeScript(afterCmd, None)(pro => { this.process = pro })
      }else true
    }

    def finish(isSuccessed: Boolean): Unit = {
      if(process != null) process.destroy()
    }

  def getColNum(cols: List[Source.Column]): Option[Int] = None
}