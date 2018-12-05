package com.kent.main

import akka.actor.Props
import akka.actor.ActorSystem
import akka.remote.ContainerFormats.ActorRef
import com.typesafe.config.ConfigFactory
import com.kent.pub.Event._
/**
 * 作为备份主节点启动
 */
object MasterStandby extends App{
  def props = Props[Master]
  var curSystem:ActorSystem = _
  var persistManager:ActorRef = _
  var emailSender: ActorRef = _
  var logRecorder: ActorRef = _
  
  val defaultConf = ConfigFactory.load()
  val masterConf = defaultConf.getString("workflow.nodes.master-standby").split(":")
  val hostConf = "akka.remote.netty.tcp.hostname=" + masterConf(0)
  val portConf = "akka.remote.netty.tcp.port=" + masterConf(1)
  
  // 创建一个Config对象
  val config = ConfigFactory.parseString(portConf)
      .withFallback(ConfigFactory.parseString(hostConf))
      .withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [${RoleType.MASTER}]"))
      .withFallback(defaultConf)
  // 创建一个ActorSystem实例
  val system = ActorSystem("akkaflow", config)
  val master = system.actorOf(Props(Master(false)), name = RoleType.MASTER)
}