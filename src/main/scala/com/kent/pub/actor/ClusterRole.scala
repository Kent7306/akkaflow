package com.kent.pub.actor

import akka.actor.{Actor, ActorPath, ActorRef, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import com.kent.pub.actor.BaseActor.ActorInfo
import com.kent.pub.actor.BaseActor.ActorType./**/_
import com.kent.pub.Event._
import com.kent.pub._
import akka.pattern.{ask, pipe}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Success

/**
 * 集群角色抽象类
 */
abstract class ClusterRole extends BaseActor {
  implicit val cluster = Cluster(context.system) 
  override val actorType = ROLE
  
  override def commonReceice = clusterReceice orElse super.commonReceice 
  
  private def clusterReceice: Actor.Receive = {
    case MemberUp(member) =>
      if (cluster.selfMember.address == member.address){
        onSelfMemberUp()
      } else {
        onOtherMemberUp(member)
      }
    case UnreachableMember(member) =>
      log.info("角色不可到达: {}", member)
    case NotifyActive(sdr) => notifyActive(sdr) pipeTo sender
    case MemberRemoved(member, previousStatus) =>
      log.info("删除角色: {} ，上一状态 {}", member.address, previousStatus)
      onMemberRemove(member)
    case state: CurrentClusterState =>
    case _:MemberEvent => // ignore
    case ShutdownCluster() =>
      cluster.leave(cluster.selfAddress)
      println(s"退出..............${self.path.name}")
      context.system.terminate()
  }

  override def preStart(): Unit = {
    // 订阅集群事件
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
    classOf[MemberUp], classOf[UnreachableMember], classOf[MemberEvent])
  }

  /**
    * 目前角色检测到有其他角色加入
    * @param member
    */
  def onOtherMemberUp(member: Member): Unit

  def onSelfMemberUp(): Unit

  /**
    * 有其他角色退出集群
    * @param member
    */
  def onMemberRemove(member: Member): Unit

  /**
    * 活动master通知
    * @param masterRef
    * @return
    */
  def notifyActive(masterRef: ActorRef): Future[Result]
  /**
   * 得到角色的ip与端口串
   */
  def getHostPortKey():Tuple2[String, Int] = {
    val host = context.system.settings.config.getString("akka.remote.artery.canonical.hostname")
    val port = context.system.settings.config.getInt("akka.remote.artery.canonical.port")
    (host,port)
  }
  /**
   * 角色加入后回调
   */
  protected def operaAfterRoleMemberUp(member: Member, roleType: String, roleActorHandler: ActorRef => Unit){
    val pathOpt = getRoleActorPath(member, roleType)
    if(pathOpt.isDefined){
      val resultF = context.actorSelection(pathOpt.get).resolveOne()
      resultF.andThen{ case Success(x) =>
        this.synchronized{
          roleActorHandler(x)
        }
      }
    }
  }

  /**
    * 获取member的角色路径
    * @param member
    * @param roleType
    * @return
    */
  protected def getRoleActorPath(member: Member, roleType: String): Option[ActorPath] = {
    if(member.hasRole(roleType)){
      Some(RootActorPath(member.address) / "user" / roleType)
    }else{
      None
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
  val MASTER = "master"
  val MASTER_STANDBY = "master-standby"
  val WORKER = "worker"
  val HTTP_SERVER = "http-server"
}