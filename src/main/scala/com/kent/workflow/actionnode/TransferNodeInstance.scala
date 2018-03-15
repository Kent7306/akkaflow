package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInstance
import akka.actor.Props
import com.kent.workflow.actionnode.transfer.Source
import com.kent.workflow.actionnode.transfer.Target
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import com.kent.pub.Event.Start
import akka.util.Timeout
import scala.concurrent.Await
import akka.actor.PoisonPill
import akka.actor.ActorRef
import com.kent.main.Worker
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.util.FileUtil
import java.io.File
import com.kent.db.LogRecorder
import scala.sys.process.ProcessLogger
import scala.sys.process._

class TransferNodeInstance(override val nodeInfo: TransferNode) extends ActionNodeInstance(nodeInfo) {  
  implicit val timeout = Timeout(3600 seconds)
  var sourceRef: ActorRef = _
  var targetRef: ActorRef = _
  private var executeProcess: Process = _
  
  def execute(): Boolean = {
    if(nodeInfo.script.isEmpty){
      executeActorTransfer()
    }else{
      executeScript(nodeInfo.script.get, None){x => this.executeProcess = x}
    }
  }
  /**
   * 使用actor进行导数
   */
  def executeActorTransfer():Boolean = {
    sourceRef = this.actionActor.context.actorOf(Props(new Source(nodeInfo.sourceInfo)),"source")
    targetRef = this.actionActor.context.actorOf(Props(new Target(nodeInfo.targetInfo, this.nodeInfo.name, this.id, sourceRef)),"target")
    try{
      val resultF = (targetRef ? Start()).mapTo[Boolean]
      val result = Await.result(resultF, 3600 seconds)
      result
    }catch{
      case e: Exception => e.printStackTrace();return false
    }
  }
  
  def kill(): Boolean = {
    if(sourceRef != null) sourceRef ! PoisonPill
    if(targetRef != null) targetRef ! PoisonPill
    if(executeProcess != null) executeProcess.destroy()
    true
  }
}