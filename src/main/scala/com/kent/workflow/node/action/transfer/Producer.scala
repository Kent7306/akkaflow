package com.kent.workflow.node.action.transfer

import akka.actor.Actor
import akka.actor.ActorRef
import akka.pattern.{ask, pipe}
import com.kent.workflow.node.action.transfer.source.Source._
import com.kent.pub.actor.BaseActor

import com.kent.workflow.node.action.transfer.source.Source

class Producer(source: Source) extends BaseActor {
  var bufferRows:List[List[String]] = null
  
  private def handleException(msg: String, sdr: ActorRef, f:() => Unit){
    try{ 
      f()
    }catch{
      case e: Exception => 
        source.errorLog(msg + "," + e.getMessage)
        sdr ! End(false)
    }
  }
  
  def individualReceive: Actor.Receive = {
    case GetColNums() =>
      handleException("初始化源数据失败",sender,() => {
        source.init()
        handleException("执行源数据查询失败",sender,() => {
          val colsOpt = source.doGetColumns
          sender ! ColNums(colsOpt)
        })
      })
    case GetRows() => handleException("装载源数据记录失败", sender, () => handleGetRows(sender))
    case End(isSuccess) => finish()
  }
  /**
   * 获取rows
   */
  def handleGetRows(sdr: ActorRef) = {
    if(bufferRows == null && !source.isEnd){  //最开始的时候
      val data = source.fillRowBuffer()
      sdr ! Rows(data)
      bufferRows = source.fillRowBuffer()
    }else if(bufferRows != null && bufferRows.size > 0){  //读取时候
      sdr ! Rows(bufferRows)
      bufferRows = source.fillRowBuffer()
    }else{  //结束时候
      (sdr ? End(true)).mapTo[Boolean]
    }
  }
  def finish() = {
    source.finish()
    context.stop(self)
  }
}

object Producer {
  def apply(source: Source):Producer = new Producer(source)
}