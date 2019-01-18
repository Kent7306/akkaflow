package com.kent.pub

import akka.cluster.Cluster
import akka.actor.ActorRef
import akka.cluster.ClusterEvent.InitialStateAsEvents
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.Member
import com.kent.pub.ClusterRole._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.RootActorPath
import scala.concurrent.duration._
import scala.util.Success
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.UnreachableMember
import com.kent.pub.ActorTool.ActorInfo
import com.kent.pub.ActorTool.ActorType._
import com.kent.pub.Event._
import akka.actor.Actor
import scala.concurrent.Future
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.CurrentClusterState

/**
 * 集群角色抽象类
 */
abstract class ClusterRole extends ActorTool {
  implicit val cluster = Cluster(context.system) 
  override val actorType = ROLE
  
  override def commonReceice = clusterReceice orElse super.commonReceice 
  
  private def clusterReceice: Actor.Receive = {
    case MemberUp(member) => 
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as Unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case state: CurrentClusterState =>
    case _:MemberEvent => // ignore 
    case ShutdownCluster() => context.system.terminate()
  }
  
  override def preStart(): Unit = {
    // 订阅集群事件
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
    classOf[MemberUp], classOf[UnreachableMember], classOf[MemberEvent])
  }
  /**
   * 得到角色的ip与端口串
   */
  def getHostPortKey():Tuple2[String, Int] = {
    val host = context.system.settings.config.getString("akka.remote.artery.canonical.hostname")
    val port = context.system.settings.config.getInt("akka.remote.artery.canonical.port")
    (host,port)
  }
  /**
   * 角色加入的操作
   */
  def operaAfterRoleMemberUp(member: Member, roleType: String, f:(ActorRef,String) => Unit){
    if(member.hasRole(roleType)){
      val path = RootActorPath(member.address) /"user" / roleType
        val resultF = context.actorSelection(path).resolveOne()
        resultF.andThen{ case Success(x) => 
          this.synchronized{
            f(x,roleType)
          }
        }
    }
  }
  override def collectActorInfo():Future[ActorInfo] = {
    val ai = new ActorInfo()
    val(ip,port) = getHostPortKey()
    ai.ip = ip
    ai.port = port
    ai.name = s"${self.path.name}(${ai.ip}:${ai.port})"
		ai.atype = this.actorType
    val caiFs = context.children.map { child => (child ? CollectActorInfo()).mapTo[ActorInfo] }.toList
    val caisF = Future.sequence(caiFs)
    caisF.map { x => ai.subActors = x ;ai}
  }
  
  /**
   * 关闭角色
   */
  def shutdownCluster(){
    context.system.terminate()
  }
}

object ClusterRole {  
  case class Request(status: String, msg: String, data: Any) extends Serializable
  case class Response(status: String, msg: String, data: Any) extends Serializable
  case class Registration() extends Serializable
  case class UnRegistration() extends Serializable
    /**
   * MASTER 状态枚举
   */
	object RStatus extends Enumeration {
		type RStatus = Value
		val R_PREPARE, R_INITING, R_INITED, R_STARTED = Value
	}
}