package com.kent.test

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props

object EmailActorTest extends App{
      val conf = """
          akka {
              actor {
                  provider = "akka.remote.RemoteActorRefProvider"
              }
              remote {
                  enabled-transports = ["akka.remote.netty.tcp"]  
                  netty.tcp {
                      hostname = "0.0.0.0"
                      port = 2551  
                  }
              }
          }
        """
    val config = ConfigFactory.parseString(conf)
  val system = ActorSystem("workflow-system", config)
  
  //
  //val pm = system.actorOf(Props[])
  
}