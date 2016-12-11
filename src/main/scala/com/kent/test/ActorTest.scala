package com.kent.test

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import scala.concurrent.duration._

object ActorTest extends App{
  val system = ActorSystem("my-system")
  val p = system.actorOf(Props[Parent],"p")
  p ! "nihao"
}

class Parent extends Actor {
  val child = context.actorOf(Props[Child],"c")
  
  def receive: Actor.Receive = {
    case a: String => 
      println("p: " + a)
      implicit val timeout = Timeout(10 seconds)
      val aa = (child ? a).mapTo[String]
      aa.map { x => println("return: " + x) }
      
  }
}

class Child extends Actor {
  def receive: Actor.Receive = {
    case a:String => 
      println("c: "+ a)
      println(sender)
      sender ! "------"
  }
}