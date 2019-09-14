package com.kent.workflow.node.action.transfer

import com.kent.workflow.node.action.transfer.target.Target

/**
  * @author kent
  * @date 2019-07-16
  * @desc
  *
  **/
class Writer(target: Target, ds: DataShare, threadName: String) extends Thread(threadName){
  var flag = true

  override def run(): Unit = {
    while (flag){
      val e = ds.get()
      e match {
        case TransferEnd() => terminate(true)
        case TransferPoison() =>
          terminate(false)
        case DataBatch(a) => persistRows(a)

      }
    }
  }

  def persistRows(rows: List[List[String]]) = {
    try {
      target.persist(rows)
      ds.incrPersistCnt(rows.size)
      target.infoLog(s"已插入${ds.getPersistCnt()}条")
    } catch {
      case e: Exception =>
        ds.handleException(new Exception(Thread.currentThread().getName + " 插入数据失败: "+e.getMessage))
        terminate(false)
    }
  }

  def terminate(isSuccess: Boolean): Unit = {
    flag = false
    target.finish(isSuccess)
  }
}
