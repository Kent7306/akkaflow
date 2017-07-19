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

abstract class ClusterRole extends ActorTool {
  // 创建一个Cluster实例
  implicit val cluster = Cluster(context.system) 
  
  override def preStart(): Unit = {
    // 订阅集群事件
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberUp], classOf[UnreachableMember], classOf[MemberEvent])
  }
  
  def getHostPortKey():String = {
    val host = context.system.settings.config.getString("akka.remote.netty.tcp.hostname")
    val port = context.system.settings.config.getString("akka.remote.netty.tcp.port")
    s"${host}:${port}"
  }
  /**
   * 角色加入的操作
   */
  def operaAfterRoleMemberUp(member: Member, roleType: String, f:(ActorRef,String) => Unit){
    if(member.hasRole(roleType)){
      val path = RootActorPath(member.address) /"user" / roleType
        val result = context.actorSelection(path).resolveOne(20 second)
        result.andThen{ 
          case Success(x) => 
            f(x,roleType)
        }
    }
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
    object ActorType extends Enumeration {
	  type ActorType = Value
			  val ROLE,DEAMO,ACTOR = Value
  }
   class ActorInfo extends Serializable{
	  import com.kent.pub.ClusterRole.ActorType._
    var name: String = _
    var ip: String = _
    var port: Int = _
    var atype: ActorType = ACTOR
    var subActors:List[ActorInfo] = List()
    
    def getClusterInfo():List[Map[String, String]] = {
	    var l:List[Map[String, String]] = List()
	    val l1 = this.subActors.map { x => 
	      val m = Map("name" -> x.name, "ip" -> x.ip, "port" -> s"${x.port}", "atype" -> s"${x.atype.id}", "pname" -> name)
	      m
	    }.toList  
	    val l2 = this.subActors.flatMap { x => x.getClusterInfo() }.toList
	    l = l ++ l1
	    l = l ++ l2
	    l
	  }
    
  }
  
  
}