package com.kent.test

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props

import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future
import scala.concurrent.duration._

object ActorTest extends App{

  val config = ConfigFactory.load("test.conf")
  val system = ActorSystem("my-system", config)
  val p = system.actorOf(Props[Parent],"parent")
  p ! "nihao"
  Thread.sleep(1500)
  p ! 2112
}

class Parent extends Actor {
  implicit val timeout = Timeout(10 seconds)
  val child = context.actorOf(Props[Child],"child")

  var str = ""
  
  def receive: Actor.Receive = {
    case a: String => 
      println("parent receive: " + a)
      val fl = List("111").map { x =>
        val bb = (child ? x).mapTo[String]
        bb
      }

      Future.sequence(fl).map{ x =>
        str += x
        println(str)
        println(x)
      }
    case b: Int =>
      println("parent receive: " + b)
      Thread.sleep(10000)
      str += "修改  "

  }
}

class Child extends Actor {
  def receive: Actor.Receive = {
    case a: String =>
      println("child receive: "+ a)
      println(sender.path)
      Thread.sleep(1000)
      sender ! a
  }
}