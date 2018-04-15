package com.kent.workflow.actionnode.transfer

import com.kent.pub.ActorTool
import akka.actor.Actor
import akka.actor.ActorRef
import com.kent.pub.Event.Start
import com.kent.workflow.actionnode.transfer.SourceObj.GetColNum
import akka.pattern.{ ask, pipe }
import com.kent.workflow.actionnode.transfer.SourceObj._
import com.kent.db.LogRecorder
import com.kent.db.LogRecorder.LogType
import scala.util.Try
import scala.util.Success

class Producer(source: Source, actionName: String, wfiId: String) extends ActorTool {
  var bufferRowsTry:Try[List[List[String]]] = null
  var isInited = false
  
  def indivivalReceive: Actor.Receive = {
    case GetColNum() => sender ! Try(source.getColNum)
    case GetRows() => handleGetRows(sender)
    case End(isSuccess) => source.finish()
  }
  
  def handleGetRows(sdr: ActorRef) = {
    if(!isInited) {
      val initTry = Try{ source.init(); List[List[String]]()}
      if(initTry.isFailure) sdr ! Rows(initTry)
    }
    
    if(bufferRowsTry == null && source.isEnd == false){  //最开始的时候
      val dataTry = Try(source.fillRowBuffer())
      sdr ! Rows(dataTry)
      bufferRowsTry = Try(source.fillRowBuffer())
    }else if(bufferRowsTry != null && bufferRowsTry.get.size > 0){  //读取时候
      sdr ! Rows(bufferRowsTry)
      bufferRowsTry = Try(source.fillRowBuffer())
    }else{  //结束时候
      sdr ! End(true)
    }
  }
  def finish() = {
    source.finish()
    context.stop(self)
  }
}

object Producer {
  def apply(source: Source, actionName: String, wfiId: String):Producer = new Producer(source, actionName, wfiId)
}