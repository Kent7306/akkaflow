package com.kent.pub
import com.kent.pub.ActorTool.ActorInfo
import com.kent.pub.ActorTool.ActorType._
import akka.actor.ActorRef
import akka.cluster.Member
import com.kent.pub.ClusterRole._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.RootActorPath
import scala.concurrent.duration._
import scala.util.Success
import com.kent.pub.ActorTool.ActorInfo
import com.kent.pub.ActorTool.ActorType._
import com.kent.pub.Event._
import akka.actor.Actor
import scala.concurrent.Future
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import com.kent.pub.ActorTool.ActorType

abstract class DaemonActor extends ActorTool{
  override val actorType = DAEMO
  
  override def collectActorInfo():Future[ActorInfo] = {
		val ai = new ActorInfo()
    ai.name = s"${self.path.name}(${self.hashCode()})"
		ai.atype = this.actorType
		val cais = context.children.map{child => 
		  val cai = new ActorInfo()
		  cai.name = s"${child.path.name}(${self.hashCode()})"
		  cai.atype = ActorType.ACTOR
		  cai
		}.toList
		ai.subActors = cais
		Future{ai}
  }
}