package com.kent.workflow.actionnode.transfer

import com.kent.pub.ActorTool
import akka.actor.Actor
import com.kent.workflow.actionnode.transfer.target.Target
import com.kent.pub.Event._
import akka.actor.ActorRef
import akka.pattern.{ ask, pipe }
import scala.concurrent.ExecutionContext.Implicits.global
import com.kent.db.LogRecorder
import com.kent.db.LogRecorder.LogType
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.util.Timeout
import com.kent.workflow.actionnode.transfer.source.Source._
import scala.util.Try

class Consumer(target: Target, producer: ActorRef) extends ActorTool { 
  var actionActorRef: ActorRef = _
  
  private def handleException(msg: String, f:() => Unit){
    try{
      f()
    }catch{
      case e: Exception => 
        target.errorLog(msg+", "+e.getMessage);
        e.printStackTrace()
        self ! End(false)
    }
  }
  def indivivalReceive: Actor.Receive = {
    case Start() => 
      actionActorRef = sender 
      handleException("初始化失败", () => {
        target.init()
        producer ! GetColNums()
      })
    case ColNums(colsOpt) => 
      //检查字段
      var tColLenOpt: Option[Int] = null
      handleException("获取目标字段数失败",() => { 
        tColLenOpt = target.getColNum(colsOpt.get) 
        handleException("执行pre语句失败", () => {
          target.preOpera()
          if(tColLenOpt.isEmpty || colsOpt.isEmpty || tColLenOpt.get == colsOpt.get.size){
        	  producer ! GetRows()        
          }else{
            target.errorLog(s"源数据字段（${colsOpt.get.size}）与目标表的字段（${tColLenOpt.get}）不等")
            self ! End(false)
          }
        })
      })
      
    case Rows(rows) => 
      handleException("插入数据失败", () => {
        target.persist(rows)
        target.infoLog(s"成功插入${target.totalRowNum}")
        producer ! GetRows() 
      })
    case End(isSuccess) =>
      var flagTmp = isSuccess
      if(flagTmp){
        try{
          target.afterOpera()
          target.infoLog(s"共插入${target.totalRowNum}条记录")
        }catch{
          case e: Exception => target.errorLog("执行after语句失败:"+e.getMessage())
          flagTmp = false
        }
      }
      target.finish(flagTmp)
      actionActorRef ! flagTmp
      producer ! End(flagTmp)
      context.stop(self)
  }
}

object Consumer {
  def apply(target: Target, producer: ActorRef):Consumer = new Consumer(target, producer)
}