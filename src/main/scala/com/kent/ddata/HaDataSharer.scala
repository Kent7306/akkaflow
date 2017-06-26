package com.kent.ddata

import akka.actor.Actor
import akka.cluster.ddata.DistributedData
import akka.cluster.Cluster
import scala.concurrent.duration._
import akka.cluster.ddata.Replicator.ReadMajority
import akka.cluster.ddata.Replicator.WriteMajority
import akka.cluster.ddata.LWWMapKey
import com.kent.workflow.WorkflowInfo
import akka.cluster.ddata.Replicator.Update
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.Replicator.UpdateSuccess
import akka.actor.ActorRef
import akka.cluster.ddata.Replicator.Get
import akka.cluster.ddata.Replicator.GetSuccess
import akka.cluster.ddata.Replicator.NotFound
import scala.collection.JavaConverters._
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.util.Timeout
import akka.pattern.{ ask, pipe }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

class HaDataSharer extends Actor {
  import com.kent.ddata.HaDataSharer._
  val replicator = DistributedData(context.system).replicator
  implicit val cluster = Cluster(context.system)
  
  val WorkflowDK = LWWMapKey[WorkflowInfo]("workflows")
  
  def receive: Actor.Receive = operaWorkflow
  
  def operaWorkflow:Receive = {
    case AddWorkflow(wf) =>
      replicator ! Update(WorkflowDK, LWWMap.empty[WorkflowInfo], writeMajority, request = Some(sender)) ( _ + (wf.name -> wf))
    case RemoveWorkflow(wfname) => 
      replicator ! Update(WorkflowDK, LWWMap.empty[WorkflowInfo], writeMajority, request = Some(sender)) ( _ - wfname)
    case UpdateSuccess(WorkflowDK, Some(replyTo: ActorRef)) => 
      println("update success")
      replyTo ! UpdateScs()
    case GetWorkflows() =>
      replicator ! Get(WorkflowDK, readMajority, request = Some(sender))
    case g @ GetSuccess(WorkflowDK, Some(replyTo: ActorRef)) =>
      val wfs = g.get(WorkflowDK).getEntries().asScala
      replyTo ! WorkflowsData(wfs)
      //val value = g.get(WorkflowDK).getElements()
    case NotFound(WorkflowDK, req) => // key counter1 does not exist
      println("NotFound")
  }
   
}

object HaDataSharer extends App{
  //#read-write-majority
  implicit val timeout1 = 5.seconds
  private val readMajority = ReadMajority(timeout1)
  private val writeMajority = WriteMajority(timeout1)
  //#read-write-majority
  
  case class AddWorkflow(wf: WorkflowInfo)
  case class RemoveWorkflow(wfname: String)
  case class GetWorkflows()
  case class UpdateScs()
  case class WorkflowsData(wfs: scala.collection.mutable.Map[String, WorkflowInfo])
  
  val defaultConf = ConfigFactory.load("test")
  val ports = List(2551,2552,2553)
  var dbs:List[ActorRef] = List()
  ports.foreach{ x =>
    val hostConf = "akka.remote.netty.tcp.hostname=127.0.0.1"
    val portConf = "akka.remote.netty.tcp.port="+x
    val config = ConfigFactory.parseString(hostConf)
        .withFallback(ConfigFactory.parseString(portConf))
        .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
        .withFallback(defaultConf)
    val system = ActorSystem("akkaflow", config)
    val db = system.actorOf(Props[HaDataSharer],"db")
    dbs = dbs :+ db
  }
  Thread.sleep(4000)
  implicit val timeout = Timeout(20 seconds)
  dbs(0) ! AddWorkflow(new WorkflowInfo("11111"))
  dbs(0) ! AddWorkflow(new WorkflowInfo("2222"))
  dbs(0) ! AddWorkflow(new WorkflowInfo("3333"))
  dbs(0) ! AddWorkflow(new WorkflowInfo("3333"))
  val a = (dbs(1) ? GetWorkflows()).mapTo[WorkflowsData]
  a.andThen{
    case Success(WorkflowsData(x)) => x.foreach{
      case (y,z) => 
      println(y)
    }
    
  }
  
}