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

class Consumer(target: Target, actionName: String, wfiId: String, producer: ActorRef) extends ActorTool { 
  var actionActorRef: ActorRef = _
  /**
   * 处理某方法的异常执行
   */
  def handleException(errorMsg: String, f:() => Boolean): Boolean = {
    try{
      val result = f()
      if(result) {
        true
      } else {
       LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, s"${errorMsg}") 
       false
      }
    }catch{
      case e: Exception => 
        //e.printStackTrace()
        LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, s"${errorMsg}: ${e.getMessage}") 
        false
    }
  }
  def indivivalReceive: Actor.Receive = {
    case Start() => 
      actionActorRef = sender 
      if( handleException("初始化失败", () => target.init())
          && handleException("执行pre语句失败", () => target.preOpera())
          && checkColNum()){
        producer ! GetRows()
      }else{
        finish(false)
      }
    case Rows(rowsTry) => 
      if(rowsTry.isFailure) {
        LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, s"source查询数据失败：${rowsTry.failed.get.getMessage}")
        finish(false)
      }
      else {
    	  val result = handleException("插入数据失败", () => target.persist(rowsTry.get))
			  if(result) {
				  LogRecorder.info(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, s"成功插入${target.totalRowNum}")
				  producer ! GetRows()
			  } else{        
				  finish(false)
			  }        
      }
    case End(isSuccess) => 
      if(isSuccess){
        val result = handleException("执行after语句失败", () => target.afterOpera())
        if(result){
          LogRecorder.info(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, s"共插入${target.totalRowNum}条记录")
        	finish(true)
        }else{
          finish(false)
        }
      }
      else 
        finish(false)
  }
  /**
   * 检查数据源与目标的字段是否相等
   */
  def checkColNum(): Boolean = {
      val tColNumOptTry = Try(target.getColNum)
      if(tColNumOptTry.isFailure){
        LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, s"获取目标数据字段数失败：${tColNumOptTry.failed.get.getMessage}")
        false
      } else{
    	  val sColNumOptTryF = (producer ? GetColNum()).mapTo[Try[Option[Int]]]
        val resultF = sColNumOptTryF.map { sColNumOptTry => 
        if(sColNumOptTry.isFailure){
           LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, s"获取源数据字段数失败：${sColNumOptTry.failed.get.getMessage}")       
           false
        }else if(sColNumOptTry.get.isEmpty || tColNumOptTry.get.isEmpty){
          true
        }else if(sColNumOptTry.get.get == tColNumOptTry.get.get){
          true
        }else{
      	  LogRecorder.error(LogType.ACTION_NODE_INSTANCE, wfiId, actionName, s"源数据字段（${sColNumOptTry.get.get}）与目标表的字段（${tColNumOptTry.get.get}）不等")       
      	  false
        }
      }
      Await.result(resultF, 20 second)
      }
  }
  /**
   * 结束
   */
  def finish(isSuccessed: Boolean){
        target.finish(isSuccessed)
        actionActorRef ! isSuccessed
        producer ! End(isSuccessed)
        context.stop(self)
  }
}

object Consumer {
  def apply(target: Target, actionName: String, wfiId: String, producer: ActorRef):Consumer = new Consumer(target, actionName, wfiId, producer)
}