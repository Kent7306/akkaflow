package com.kent.daemon

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.{DistributedData, LWWMap, LWWMapKey}
import akka.cluster.ddata.Replicator._
import akka.pattern.ask
import akka.util.Timeout
import com.kent.pub.actor.Daemon
import com.kent.workflow.{Workflow, WorkflowInstance}
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Success

class HaDataStorager extends Daemon{
  import com.kent.daemon.HaDataStorager._
  //#read-write-majority
  implicit val timeout1 = 5.seconds
  private val readMajority = ReadMajority(timeout1)
  private val writeMajority = WriteMajority(timeout1)
  //#read-write-majority
  
  val replicator = DistributedData(context.system).replicator
  implicit val cluster = Cluster(context.system)
  //以工作流的name作为key
  val WorkflowDK = LWWMapKey[String, Workflow]("workflows")
  //以等待队列中的工作流实例id作为key
  val RWFIDK = LWWMapKey[String, WorkflowInstance]("RWFIids")
  //以等待队列中的工作流实例id作为key
  val WWFIDK = LWWMapKey[String, WorkflowInstance]("WWFIids")
  //xml文件信息
  val XmlFileDK = LWWMapKey[String, Long]("xmlFiles")
  //master actor信息
  val RoleDK = LWWMapKey[String, RoleContent]("roles")
  /*override def preStart(): Unit = {
    // 订阅集群事件
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberUp], classOf[UnreachableMember], classOf[MemberEvent])
  }*/
  
  def individualReceive: Actor.Receive = operaWorkflow orElse
                               operaRWFI orElse 
                               operaWWFI orElse
                               operaXmlFile orElse
                               operareRole

   /**
   * 工作流存储操作
   */
  def operaWorkflow:Receive = {
    case AddWorkflow(wf) =>
      replicator ! Update(WorkflowDK, LWWMap.empty[String, Workflow], writeMajority, request = Some(sender)) ( _ + (wf.name -> wf))
    case RemoveWorkflow(wfname) => 
      replicator ! Update(WorkflowDK, LWWMap.empty[String, Workflow], writeMajority, request = Some(sender)) ( _ - wfname)
    case UpdateSuccess(WorkflowDK, Some(replyTo: ActorRef)) => 
      //println("update success wf")
    case GetWorkflows() =>
      replicator ! Get(WorkflowDK, readMajority, request = Some(sender))
    case g @ GetSuccess(WorkflowDK, Some(replyTo: ActorRef)) =>
      val wfs = g.get(WorkflowDK).getEntries().asScala.map(_._2).toList
      replyTo ! wfs
    case NotFound(WorkflowDK, Some(replyTo: ActorRef)) => // key workflows does not exist
      //println("NotFound wf")
      replyTo ! List[Workflow]()
  }
  /**
   * 运行中工作流实例存储操作
   */
  def operaRWFI:Receive = {
    case AddRWFI(wfi) =>
      replicator ! Update(RWFIDK, LWWMap.empty[String, WorkflowInstance], writeMajority, request = Some(sender)) ( _ + (wfi.id -> wfi))
    case RemoveRWFI(wfiId) =>
      replicator ! Update(RWFIDK, LWWMap.empty[String, WorkflowInstance], writeMajority, request = Some(sender)) ( _ - wfiId)
    case UpdateSuccess(RWFIDK, Some(replyTo: ActorRef)) => 
      //println("update success RWFI")
    case GetRWFIs() =>
      replicator ! Get(RWFIDK, readMajority, request = Some(sender))
    case g @ GetSuccess(RWFIDK, Some(replyTo: ActorRef)) =>
      val wfis = g.get(RWFIDK).getEntries().asScala.map(_._2).toList
      replyTo ! wfis
    case NotFound(RWFIDK, Some(replyTo: ActorRef)) => // key workflows does not exist
      //println("NotFound RWFIDK")
      replyTo ! List[WorkflowInstance]()
  }
  /**
   * 等待中的工作流实例存储操作
   */
  def operaWWFI:Receive = {
    case AddWWFI(wfi) =>
      replicator ! Update(WWFIDK, LWWMap.empty[String, WorkflowInstance], writeMajority, request = Some(sender)) ( _ + (wfi.id -> wfi))
    case RemoveWWFI(wfiId) =>
      replicator ! Update(WWFIDK, LWWMap.empty[String, WorkflowInstance], writeMajority, request = Some(sender)) ( _ - wfiId)
    case UpdateSuccess(WWFIDK, Some(replyTo: ActorRef)) => 
      //println("update success WWFI")
    case GetWWFIs() =>
      replicator ! Get(WWFIDK, readMajority, request = Some(sender))
    case g @ GetSuccess(WWFIDK, Some(replyTo: ActorRef)) =>
      val wfis = g.get(WWFIDK).getEntries().asScala.map(_._2).toList
      replyTo ! wfis
    case NotFound(WWFIDK, Some(replyTo: ActorRef)) => // key workflows does not exist
      //println("NotFound WWFIDK")
      replyTo ! List[WorkflowInstance]()
  }
  /**
   * 已经解析的xml文件信息
   */
  def operaXmlFile:Receive = {
    case AddXmlFile(filename, lastModTime) =>
      replicator ! Update(XmlFileDK, LWWMap.empty[String, Long], writeMajority, request = Some(sender)) ( _ + (filename -> lastModTime))
    case RemoveXmlFile(filename) =>
      replicator ! Update(XmlFileDK, LWWMap.empty[String, Long], writeMajority, request = Some(sender)) ( _ - filename)
    case UpdateSuccess(XmlFileDK, Some(replyTo: ActorRef)) => 
      //println("update success XmlFile")
    case GetXmlFiles() =>
      replicator ! Get(XmlFileDK, readMajority, request = Some(sender))
    case g @ GetSuccess(XmlFileDK, Some(replyTo: ActorRef)) =>
      val xmlfiles = g.get(XmlFileDK).getEntries().asScala.toMap
      replyTo ! xmlfiles
    case NotFound(XmlFileDK, Some(replyTo: ActorRef)) => // key workflows does not exist
      //println("NotFound XmlFileDK")
      replyTo ! Map[String, Long]()
  }
  /**
   * 各个角色的actor信息
   */
  //implicit def path2String(p: ActorPath):String = p.toString()
  def operareRole:Receive = {
    case AddRole(hostPortKey, sdr, roleType) =>
      replicator ! Update(RoleDK, LWWMap.empty[String, RoleContent], writeMajority, request = Some(sender)) ( 
          _ + (hostPortKey -> RoleContent(roleType,sdr)))
    case removeRole(hostPortKey) =>
      replicator ! Update(RoleDK, LWWMap.empty[String, RoleContent], writeMajority, request = Some(sender)) ( _ - hostPortKey)
    case UpdateSuccess(RoleDK, Some(replyTo: ActorRef)) => 
    case GetRoles() =>
      replicator ! Get(RoleDK, readMajority, request = Some(sender))
    case g @ GetSuccess(RoleDK, Some(replyTo: ActorRef)) =>
      val roles = g.get(RoleDK).getEntries().asScala.toMap
      replyTo ! roles
    case NotFound(RoleDK, Some(replyTo: ActorRef)) => // key workflows does not exist
      replyTo ! Map[String, RoleContent]()
  }
  
}
object HaDataStorager extends App{
  
  case class RoleContent(roleType: String, sdr:ActorRef)
  case class AddRole(hostPortKey: String, sdr: ActorRef, roleType:String)
  case class removeRole(hostPortKey: String)
  case class GetRoles()
  
  case class AddWorkflow(wf: Workflow)
  case class RemoveWorkflow(wfname: String)
  case class GetWorkflows()
  
  case class AddRWFI(wfi: WorkflowInstance)
  case class RemoveRWFI(wfiId: String)
  case class GetRWFIs()
  
  case class AddWWFI(wfi: WorkflowInstance)
  case class RemoveWWFI(wfIid: String)
  case class GetWWFIs() 
  
  case class AddXmlFile(filename: String, lastModTime: Long)
  case class RemoveXmlFile(filename: String)
  case class GetXmlFiles()
  
  //获取到的分布式数据集合
  case class DistributeData(
      workflows: List[Workflow],
      runningWfis: List[WorkflowInstance],
      wattingWfis: List[WorkflowInstance],
      xmlFiles: Map[String, Long]
  )
  
  
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
  //dbs(0) ! AddWorkflow(new Workflow("11111"))
  //dbs(0) ! AddWorkflow(new Workflow("2222"))
  //dbs(0) ! AddWorkflow(new Workflow("3333"))
  //dbs(0) ! AddWorkflow(new Workflow("3333"))
  //Thread.sleep(1000)
  (dbs(1) ? GetWorkflows()).mapTo[List[Workflow]].andThen{case Success(x) => x.foreach{y => println(y.name)}}
}