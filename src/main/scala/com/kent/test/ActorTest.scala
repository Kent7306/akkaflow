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
  implicit val timeout = Timeout(10 seconds)
  val child = context.actorOf(Props[Child],"c")
  
  def receive: Actor.Receive = {
    case a: String => 
      println("p: " + a)
      val aa = (child ? "111").mapTo[String]
      val bb = (child ? "222").mapTo[String]
    	val cc = (child ? "333").mapTo[String]
    	val dd = (child ? "444").mapTo[String]
      aa.map { x => println("return: " + x) }
      bb.map { x => println("return: " + x) }
      cc.map { x => println("return: " + x) }
      dd.map { x => println("return: " + x) }
      
  }
}

class Child extends Actor {
  def receive: Actor.Receive = {
    case a:String => 
      println("c: "+ a)
      Thread.sleep(3000)
      //println(sender.path.address.host+"*****")
      sender ! a
  }
}