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
import com.kent.coordinate.Coordinator
import com.kent.workflow.WorkflowInstance

class HaDataStorager extends Actor {
  import com.kent.ddata.HaDataStorager._
  val replicator = DistributedData(context.system).replicator
  implicit val cluster = Cluster(context.system)
  //以工作流的name作为key
  val WorkflowDK = LWWMapKey[WorkflowInfo]("workflows")
  //以调度器的name作为key
  val CoordinatorDK = LWWMapKey[Coordinator]("Coordinators")
  //以运行中的工作流实例id作为key
  val RWFIDK = LWWMapKey[String]("RWFIids")
  //以等待队列中的工作流实例id作为key
  val WWFIDK = LWWMapKey[WorkflowInstance]("WWFIids")
  
  def receive: Actor.Receive = operaWorkflow orElse operaCoordinator orElse operaRWFIid orElse operaWWFI
  
  /**
   * 工作流存储操作
   */
  def operaWorkflow:Receive = {
    case AddWorkflow(wf) =>
      replicator ! Update(WorkflowDK, LWWMap.empty[WorkflowInfo], writeMajority, request = Some(sender)) ( _ + (wf.name -> wf))
    case RemoveWorkflow(wfname) => 
      replicator ! Update(WorkflowDK, LWWMap.empty[WorkflowInfo], writeMajority, request = Some(sender)) ( _ - wfname)
    case UpdateSuccess(WorkflowDK, Some(replyTo: ActorRef)) => 
      println("update success wf")
    case GetWorkflows() =>
      replicator ! Get(WorkflowDK, readMajority, request = Some(sender))
    case g @ GetSuccess(WorkflowDK, Some(replyTo: ActorRef)) =>
      val wfs = g.get(WorkflowDK).getEntries().asScala.map(_._2).toList
      replyTo ! wfs
    case NotFound(WorkflowDK, req) => // key workflows does not exist
      println("NotFound wf")
  }
  /**
   * 调度器存储操作
   */
  def operaCoordinator:Receive = {
    case AddCoordinator(coor) =>
      replicator ! Update(CoordinatorDK, LWWMap.empty[Coordinator], writeMajority, request = Some(sender)) ( _ + (coor.name -> coor))
    case RemoveCoordinator(coorName) =>
      replicator ! Update(CoordinatorDK, LWWMap.empty[Coordinator], writeMajority, request = Some(sender)) ( _ - coorName)
    case UpdateSuccess(CoordinatorDK, Some(replyTo: ActorRef)) => 
      println("update success coor")
    case GetCoordinators() =>
      replicator ! Get(CoordinatorDK, readMajority, request = Some(sender))
    case g @ GetSuccess(CoordinatorDK, Some(replyTo: ActorRef)) =>
      val wfs = g.get(CoordinatorDK).getEntries().asScala.map(_._2).toList
      replyTo ! wfs
    case NotFound(CoordinatorDK, req) => // key workflows does not exist
      println("NotFound coor")
  }
  /**
   * 运行中工作流实例存储操作
   */
  def operaRWFIid:Receive = {
    case AddRWFIid(wfiId) =>
      replicator ! Update(RWFIDK, LWWMap.empty[String], writeMajority, request = Some(sender)) ( _ + (wfiId -> wfiId))
    case RemoveRWFIid(wfiId) =>
      replicator ! Update(RWFIDK, LWWMap.empty[String], writeMajority, request = Some(sender)) ( _ - wfiId)
    case UpdateSuccess(RWFIDK, Some(replyTo: ActorRef)) => 
      println("update success RWFI")
    case GetRWFIids() =>
      replicator ! Get(RWFIDK, readMajority, request = Some(sender))
    case g @ GetSuccess(RWFIDK, Some(replyTo: ActorRef)) =>
      val wfs = g.get(RWFIDK).getEntries().asScala.map(_._2).toList
      replyTo ! wfs
    case NotFound(RWFIDK, req) => // key workflows does not exist
      println("NotFound RWFIDK")
  }
  /**
   * 运行中工作流实例存储操作
   */
  def operaWWFI:Receive = {
    case AddWWFI(wfi) =>
      replicator ! Update(WWFIDK, LWWMap.empty[WorkflowInstance], writeMajority, request = Some(sender)) ( _ + (wfi.id -> wfi))
    case RemoveWWFI(wfiId) =>
      replicator ! Update(WWFIDK, LWWMap.empty[WorkflowInstance], writeMajority, request = Some(sender)) ( _ - wfiId)
    case UpdateSuccess(WWFIDK, Some(replyTo: ActorRef)) => 
      println("update success WWFI")
    case GetWWFIs() =>
      replicator ! Get(WWFIDK, readMajority, request = Some(sender))
    case g @ GetSuccess(WWFIDK, Some(replyTo: ActorRef)) =>
      val wfis = g.get(WWFIDK).getEntries().asScala.map(_._2).toList
      replyTo ! wfis
    case NotFound(WWFIDK, req) => // key workflows does not exist
      println("NotFound RWFIDK")
  }
   
}
object HaDataStorager extends App{
  //#read-write-majority
  implicit val timeout1 = 5.seconds
  private val readMajority = ReadMajority(timeout1)
  private val writeMajority = WriteMajority(timeout1)
  //#read-write-majority
  
  case class AddWorkflow(wf: WorkflowInfo)
  case class RemoveWorkflow(wfname: String)
  case class GetWorkflows()

  case class AddRWFIid(wfiId: String)
  case class RemoveRWFIid(wfiId: String)
  case class GetRWFIids()
  
  case class AddCoordinator(coor: Coordinator)
  case class RemoveCoordinator(coorName: String)
  case class GetCoordinators()
  
  case class AddWWFI(wfi: WorkflowInstance)
  case class RemoveWWFI(wfIid: String)
  case class GetWWFIs() 
  
  
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
    val db = system.actorOf(Props[HaDataStorager],"db")
    dbs = dbs :+ db
  }
  Thread.sleep(4000)
  implicit val timeout = Timeout(20 seconds)
  /*dbs(0) ! AddWorkflow(new WorkflowInfo("11111"))
  dbs(0) ! AddWorkflow(new WorkflowInfo("2222"))
  dbs(0) ! AddWorkflow(new WorkflowInfo("3333"))
  dbs(0) ! AddWorkflow(new WorkflowInfo("3333"))
  Thread.sleep(1000)
  (dbs(1) ? GetWorkflows()).mapTo[List[WorkflowInfo]].andThen{case Success(x) => x.foreach{y => println(y.name)}}*/
  
  dbs(0) ! AddRWFIid("111")
  dbs(0) ! AddRWFIid("121")
  dbs(0) ! AddRWFIid("311")
  dbs(0) ! AddRWFIid("111")
  Thread.sleep(1000)
  (dbs(1) ? GetRWFIids()).mapTo[List[String]].andThen{case Success(x) => x.foreach{y => println(y)}}
}