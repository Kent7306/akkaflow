package com.kent.workflow.node.action.transfer.target

import com.kent.daemon.LogRecorder
import com.kent.daemon.LogRecorder.LogType._
import com.kent.pub.db.Column

/**
  * 目标
  */
abstract class Target(actionName: String, instanceId: String){
  //记录当前导入的数据量
  var totalRowNum = 0
  //目标字段个数
  var columns: Option[List[Column]] = None

  doInit()

  def doInit(): Unit ={
    try {
      init()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw new Exception("初始化目标库失败: "+e.getMessage)
    }
  }
  /**
   * 初始化
   */
  def init(): Boolean

  /**
    * 初始化目标字段
    * @param sourceCols
    */
  def initColWithSourceCols(sourceCols: List[Column]):Option[List[Column]] = {
    columns = getColsWithSourceCols(sourceCols)
    columns
  }
  /**
    * 获取目标字段个数
    * 如果是数据库源，会检测并建表
    * @param sourceCols
    * @return
    */
  def getColsWithSourceCols(sourceCols: List[Column]): Option[List[Column]]
  /**
   * 批量导入数据
   */
  def persist(rows: List[List[String]]): Boolean

  /**
   * 导入数据前，用户自定义操作
   */
  def preOpera(): Boolean
  /**
   * 导入数据完成后，用户自定义操作
   */
  def afterOpera(): Boolean
  /**
   * 导数结束
   */
  def finish(isSuccessed: Boolean)

  def infoLog(line: String): Unit = LogRecorder.info(ACTION_NODE_INSTANCE, instanceId, actionName, line)
  def errorLog(line: String): Unit = LogRecorder.error(ACTION_NODE_INSTANCE, instanceId, actionName, line)
  def warnLog(line: String): Unit = LogRecorder.warn(ACTION_NODE_INSTANCE, instanceId, actionName, line)
}