package com.kent.workflow.node.action.transfer

import com.kent.workflow.node.action.transfer.source.Source

/**
  * @author kent
  * @date 2019-07-16
  * @desc
  *
  **/
class Reader(source: Source, ds: DataShare, threadName: String) extends Thread(threadName){
  var flag = true
  override def run(): Unit = {
    while (!source.isEnd && flag) {
      flag = if (ds.exceptionOpt.isEmpty) {
        fillBatch()
      } else {
        false
      }
    }
    if (source.isEnd){
      ds.put(TransferEnd())
    }
    source.finish()
  }

  /**
    * 当如batch
    * @return
    */
  def fillBatch(): Boolean = {
    try {
      val data = source.doFillRowBuffer()
      ds.put(DataBatch(data))
      true
    } catch {
      case e: Exception =>
        ds.handleException(e)
        false
    }
  }
}
