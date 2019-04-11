package com.kent.pub.actor

import com.kent.pub.actor.BaseActor.{ActorInfo, ActorType}
import com.kent.pub.actor.BaseActor.ActorType._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
	* 常驻Actor
	*/
abstract class Daemon extends BaseActor{
  override val actorType = DEAMON
  
  override def collectActorInfo():Future[ActorInfo] = {
		val ai = new ActorInfo()
    ai.name = s"${self.path.name}(${self.hashCode()})"
		ai.atype = this.actorType
		val cais = context.children.map{child => 
		  val cai = new ActorInfo()
		  cai.name = s"${child.path.name}(${self.hashCode()})"
		  cai.atype = ActorType.BASE
		  cai
		}.toList
		ai.subActors = cais
		Future{ai}
  }
}