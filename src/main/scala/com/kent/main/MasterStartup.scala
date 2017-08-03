package com.kent.main

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import com.kent.pub.Event._
import akka.actor.Props
/**
 * 作为活动主节点启动（若已存在活动主节点，则把该节点设置为备份主节点）
 */
object MasterStartup extends App{
  val defaultConf = ConfigFactory.load()
  val masterConf = defaultConf.getString("workflow.nodes.master").split(":")
  val hostConf = "akka.remote.netty.tcp.hostname=" + masterConf(0)
  val portConf = "akka.remote.netty.tcp.port=" + masterConf(1)
  
  // 创建一个Config对象
  val config = ConfigFactory.parseString(portConf)
      .withFallback(ConfigFactory.parseString(hostConf))
      .withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [${RoleType.MASTER}]"))
      .withFallback(defaultConf)
  // 创建一个ActorSystem实例
  val system = ActorSystem("akkaflow", config)
  val master = system.actorOf(Props(Master(true)), name = RoleType.MASTER)
}