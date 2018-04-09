package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInstance
import akka.actor.Props
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
import com.kent.workflow.actionnode.transfer.SourceObj.Source
import com.kent.workflow.actionnode.transfer.Producer
import com.kent.workflow.actionnode.transfer.SourceObj._
import com.kent.workflow.actionnode.transfer.TargetObj._
import com.kent.workflow.actionnode.transfer.Consumer

class TransferNodeInstance(override val nodeInfo: TransferNode) extends ActionNodeInstance(nodeInfo) {  
  implicit val timeout = Timeout(3600 seconds)
  var producer: ActorRef = _
  var consumer: ActorRef = _
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
    val source: Source = if(nodeInfo.dbsInfOpt.isDefined){  //DB source
      val dbLinkF = actionActor.getDBLink(nodeInfo.dbsInfOpt.get.dbLinkName)
      val dbLink = Await.result(dbLinkF, 20 seconds)
      if(dbLink.isEmpty){
        errorLog("source中未找到对应的db-link配置")
        null
      }else {
        new DBSource(dbLink.get, nodeInfo.dbsInfOpt.get.query)
      }
    } else if(nodeInfo.fsInfOpt.get.path.toLowerCase().contains("hdfs:")){  //HDFS source
      val fsInf = nodeInfo.fsInfOpt.get
      new HdfsSource(fsInf.delimited, fsInf.path)
    } else {  // local file source
      val fsInf = nodeInfo.fsInfOpt.get
      new LocalFileSource(fsInf.delimited, fsInf.path)
    }
    
    
  if(source == null) return false
 
  val target: Target = if(nodeInfo.dbtInfOpt.isDefined){  //DB target
      val dbtInf = nodeInfo.dbtInfOpt.get
      val dbLinkF = actionActor.getDBLink(dbtInf.dbLinkName)
      val dbLinkOpt = Await.result(dbLinkF, 20 seconds)
      if(dbLinkOpt.isEmpty){
        errorLog("target中未找到对应的db-link配置")
        null
      }else{
        new DBTarget(dbtInf.isPreTruncate, dbLinkOpt.get, dbtInf.table, dbtInf.preSql, dbtInf.afterSql)
      }
   } else if(nodeInfo.ftInfOpt.get.path.toLowerCase().matches("hdfs:")){  //HDFS target
     val ftInf = nodeInfo.ftInfOpt.get
     new HdfsTarget(ftInf.isPreDel,ftInf.delimited,ftInf.path, ftInf.preCmd, ftInf.afterCmd, this)
   } else {  //local file target
     val ftInf = nodeInfo.ftInfOpt.get
     new LocalFileTarget(ftInf.isPreDel,ftInf.delimited,ftInf.path, ftInf.preCmd, ftInf.afterCmd, this)
   }
  if(target == null) return false
  
  producer = this.actionActor.context.actorOf(Props(Producer(source, nodeInfo.name, this.id)),"producer")
  consumer = this.actionActor.context.actorOf(Props(Consumer(target, nodeInfo.name, this.id, producer)),"target")
    try{
      val resultF = (consumer ? Start()).mapTo[Boolean]
      val result = Await.result(resultF, 3600 seconds)
      result
    }catch{
      case e: Exception => e.printStackTrace();return false
    }
  }
  
  def kill(): Boolean = {
    if(consumer != null) consumer ! End(false)
    if(executeProcess != null) executeProcess.destroy()
    true
  }
}