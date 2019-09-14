package com.kent.workflow.node.action.transfer

import akka.actor.Actor
import com.kent.workflow.node.action.transfer.target.Target
import com.kent.pub.Event._
import akka.actor.ActorRef
import akka.pattern.{ask, pipe}

import scala.concurrent.ExecutionContext.Implicits.global
import com.kent.daemon.LogRecorder.LogType

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.util.Timeout
import com.kent.daemon.LogRecorder
import com.kent.pub.actor.BaseActor
import com.kent.pub.db.Column
import com.kent.workflow.node.action.transfer.source.Source._

import scala.util.Try

class Consumer(target: Target, producer: ActorRef) extends BaseActor {
  var actionActorRef: ActorRef = _
  
  private def handleException(msg: String, f:() => Unit){
    try{
      f()
    }catch{
      case e: Exception => 
        target.errorLog(msg+", "+e.getMessage)
        e.printStackTrace()
        terminate(false, None)
    }
  }
  def individualReceive: Actor.Receive = {
    case Start() => start(sender)
    case ColNums(colsOpt) => handleCol(colsOpt)
    case Rows(rows) => persistRows(rows)
    case End(isSuccess) =>
      terminate(isSuccess, Some(sender))
  }

  def handleCol(colsOpt: Option[List[Column]]): Unit ={
    //检查字段
    var tColLenOpt: Option[Int] = null
    handleException("执行pre语句失败",() => {
      target.preOpera()
      handleException("获取目标字段数失败", () => {
       // tColLenOpt = target.get(colsOpt.get)
        tColLenOpt = Some(0)
        if(tColLenOpt.isEmpty || colsOpt.isEmpty || tColLenOpt.get == colsOpt.get.size){
          producer ! GetRows()
        }else{
          target.errorLog(s"源数据字段（${colsOpt.get.size}）与目标表的字段（${tColLenOpt.get}）不等")
          this.terminate(false, None)
        }
      })
    })
  }

  def start(sdr: ActorRef): Unit = {
    actionActorRef = sdr
    handleException("初始化失败", () => {
      target.init()
      producer ! GetColNums()
    })
  }

  def persistRows(rows: List[List[String]]) = {
    handleException("插入数据失败", () => {
      target.persist(rows)
      target.infoLog(s"成功插入${target.totalRowNum}")
      producer ! GetRows()
    })
  }

  def terminate(isSuccess: Boolean ,senderOpt: Option[ActorRef]): Unit = {
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
    producer ! End(flagTmp)
    actionActorRef ! flagTmp
    if(senderOpt.isDefined) senderOpt.get ! true
    context.stop(self)
  }

}

object Consumer {
  def apply(target: Target, producer: ActorRef):Consumer = new Consumer(target, producer)
}