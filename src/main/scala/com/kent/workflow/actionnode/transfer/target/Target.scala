package com.kent.workflow.actionnode.transfer.target

import com.kent.workflow.actionnode.transfer.source.Source.Column
import com.kent.db.LogRecorder
import com.kent.db.LogRecorder.LogType._

trait Target{
  var totalRowNum = 0
  //日志
  var actionName: String = _
  var instanceId: String = _
  def infoLog(line: String) = LogRecorder.info(ACTION_NODE_INSTANCE, instanceId, actionName, line) 
  def errorLog(line: String) = LogRecorder.error(ACTION_NODE_INSTANCE, instanceId, actionName, line)
  def warnLog(line: String) = LogRecorder.warn(ACTION_NODE_INSTANCE, instanceId, actionName, line)
  /**
   * 初始化
   */
  def init(): Boolean
  /**
   * 获取目标字段个数
   */
  def getColNum(cols: List[Column]): Option[Int]
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
}