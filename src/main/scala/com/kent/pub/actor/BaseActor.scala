package com.kent.pub.actor

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.kent.pub.Event._
import com.kent.pub.actor.BaseActor.ActorInfo

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
/**
 * Actor基类
 */
abstract class BaseActor extends Actor with ActorLogging{
  import com.kent.pub.actor.BaseActor.ActorType._
  implicit val timeout = Timeout(20 seconds)
  val askTimeout = 20 seconds
  val actorType = BASE
  
  def receive: Actor.Receive = individualReceive orElse commonReceice
  /**
   * 私有消息事件处理
   */
  def individualReceive: Actor.Receive
  /**
   * 公共消息事件处理
   */
  def commonReceice: Actor.Receive = {
    case CollectActorInfo() => collectActorInfo() pipeTo sender
    case x => log.error(s"${self.path}接收到发送方${sender.path}未知消息:${x}")
  }

  /**
   * 该actor包括子actor的信息
   */
  def collectActorInfo():Future[ActorInfo] = {
		val ai = new ActorInfo()
    ai.name = s"${self.path.name}"
		ai.atype = this.actorType
    val caiFs = context.children.map { child => (child ? CollectActorInfo()).mapTo[ActorInfo]}.toList
    val caisF = Future.sequence(caiFs)
    caisF.map { x => ai.subActors = x ;ai}
  }
}
/**
 * Actor类型
 */
object BaseActor {
  class ActorInfo extends Serializable{
    import  ActorType._
    var name: String = _
    var ip: String = _
    var port: Int = _
    var atype: ActorType = BASE
    var subActors:List[ActorInfo] = List()
    /**
     * 获取actor层级信息
     */
    def toMapList():List[Map[String, String]] = {
	    var l:List[Map[String, String]] = List()
	    val l1 = this.subActors.map { x =>
	      Map("name" -> x.name, "ip" -> x.ip, "port" -> s"${x.port}",
            "atype" -> s"${x.atype.id}", "pname" -> name)
	    }
	    val l2 = this.subActors.flatMap { x => x.toMapList() }.toList
	    l = l ++ l1
	    l = l ++ l2
	    l
	  }
  }
  object ActorType extends Enumeration {
    type ActorType = Value
    val ROLE,DEAMON,BASE = Value
  }
}