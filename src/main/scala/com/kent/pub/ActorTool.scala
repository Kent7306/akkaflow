package com.kent.pub

import akka.actor.Actor
import akka.actor.ActorLogging
import com.kent.pub.Event._
import com.kent.pub.ClusterRole._
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.Await
import com.kent.pub.ActorTool.ActorInfo
/**
 * Actor扩展类
 */
abstract class ActorTool extends Actor with ActorLogging{
	import com.kent.pub.ActorTool.ActorType._
  implicit val timeout = Timeout(20 seconds)
  val askTimeout = 20 seconds
  val actorType = ACTOR
  
  def receive: Actor.Receive = indivivalReceive orElse commonReceice
  /**
   * 私有消息事件处理
   */
  def indivivalReceive: Actor.Receive
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
object ActorTool {
    object ActorType extends Enumeration {
	  type ActorType = Value
			  val ROLE,DAEMO,ACTOR = Value
  }
  class ActorInfo extends Serializable{
	  import com.kent.pub.ActorTool.ActorType._
    var name: String = _
    var ip: String = _
    var port: Int = _
    var atype: ActorType = ACTOR
    var subActors:List[ActorInfo] = List()
    /**
     * 获取actor层级信息
     */
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