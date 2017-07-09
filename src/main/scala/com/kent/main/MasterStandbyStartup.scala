package com.kent.main

import akka.actor.Props
import akka.actor.ActorSystem
import akka.remote.ContainerFormats.ActorRef
import com.typesafe.config.ConfigFactory
import com.kent.pub.Event._

object MasterStandbyStartup extends App{
  def props = Props[Master]
  var curSystem:ActorSystem = _
  var persistManager:ActorRef = _
  var emailSender: ActorRef = _
  var logRecorder: ActorRef = _
  
  val defaultConf = ConfigFactory.load()
  val masterConf = defaultConf.getString("workflow.nodes.master_standby").split(":")
  val hostConf = "akka.remote.netty.tcp.hostname=" + masterConf(0)
  val portConf = "akka.remote.netty.tcp.port=" + masterConf(1)
  
  // 创建一个Config对象
  val config = ConfigFactory.parseString(portConf)
      .withFallback(ConfigFactory.parseString(hostConf))
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
      .withFallback(defaultConf)
  // 创建一个ActorSystem实例
  val system = ActorSystem("akkaflow", config)
  Master.config =config
  Master.system = system
  val master = system.actorOf(Master.props, name = "master")
  master ! StartIfActive(false)
}