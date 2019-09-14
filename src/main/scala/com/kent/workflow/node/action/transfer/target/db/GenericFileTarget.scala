package com.kent.workflow.node.action.transfer.target.db

import java.io.BufferedWriter

import com.kent.pub.db.Column
import com.kent.pub.io.{FileLink, FileSystemOperator}
import com.kent.workflow.node.action.ActionNodeInstance
import com.kent.workflow.node.action.transfer.target.Target

import scala.sys.process.Process

/**
  * @author kent
  * @date 2019-08-27
  * @desc
  *
  **/
class GenericFileTarget(fl: FileLink, delimited: String, path: String, isPreDel: Boolean,
                        preCmd: String, afterCmd: String,
                        instanceId: String, actionName: String, actionInstance: ActionNodeInstance) extends Target(actionName, instanceId) {
  var process: Process = null
  var bw: BufferedWriter = _
  var operator: FileSystemOperator = _
  /**
    * 初始化
    */
  override def init(): Boolean = {
    operator = fl.getOperator()
    bw = operator.getBufferedWriter(path)
    true
  }

  /**
    * 获取目标字段个数
    *
    * @param sourceCols
    * @return
    */
  override def getColsWithSourceCols(sourceCols: List[Column]): Option[List[Column]] = None

  /**
    * 批量导入数据
    */
  override def persist(rows: List[List[String]]): Boolean = {
    val lines = rows.map { x => x.mkString(delimited) }
    totalRowNum += lines.size
    lines.foreach { x => bw.write(x+"\n") }
    bw.flush()
    true
  }

  /**
    * 导入数据前，用户自定义操作
    */
  override def preOpera(): Boolean = {
    if(preCmd != null){
      actionInstance.executeScript(preCmd, None)(pro => {this.process = pro })
    }else true
  }

  /**
    * 导入数据完成后，用户自定义操作
    */
  override def afterOpera(): Boolean = {
    if(afterCmd != null){
      actionInstance.executeScript(afterCmd, None)(pro => { this.process = pro })
    }else true
  }

  /**
    * 导数结束
    */
  override def finish(isSuccessed: Boolean): Unit = {
    if(bw != null) bw.close()
    if(operator != null) operator.close()
    if(process != null) process.destroy()
  }
}
