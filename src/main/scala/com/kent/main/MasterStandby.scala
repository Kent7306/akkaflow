package com.kent.main

import akka.actor.Props
import akka.actor.ActorSystem
import akka.remote.ContainerFormats.ActorRef
import com.typesafe.config.ConfigFactory
import com.kent.pub.Event._
import com.kent.pub.actor.ClusterRole
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
  val port = defaultConf.getInt("workflow.node.master.standby.port")
  val portConf = "akka.remote.artery.canonical.port=" + port
  val portBindConf = "akka.remote.artery.bind.port=" + port
  
  // 创建一个Config对象
  val config = ConfigFactory.parseString(portConf)
      .withFallback(ConfigFactory.parseString(portBindConf))
      .withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [${ClusterRole.MASTER}]"))
      .withFallback(defaultConf)
  // 创建一个ActorSystem实例
  val system = ActorSystem("akkaflow", config)
  val master = system.actorOf(Props(Master(false)), name = ClusterRole.MASTER)
}