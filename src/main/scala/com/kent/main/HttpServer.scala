package com.kent.main

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import scala.io.StdIn
import com.typesafe.config.ConfigFactory
import com.kent.pub.ShareData
import akka.actor.Actor
import akka.cluster.ClusterEvent._
import com.kent.main.ClusterRole.Registration
import akka.actor.Props

class HttpServer extends ClusterRole {
  def receive: Actor.Receive = {
    case MemberUp(member) => 
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as Unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case state: CurrentClusterState =>
    
    case _:MemberEvent => // ignore 
      
    case Registration() => {
      //worker请求注册
      context watch sender
      roler = roler :+ sender
      log.info("Master registered: " + sender)
    }
  }
}
object HttpServer extends App{
  def props = Props[HttpServer]
  val defaultConf = ConfigFactory.load()
  val httpConf = defaultConf.getStringList("workflow.nodes.http-servers").get(0).split(":")
  
  // 创建一个Config对象
  val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + httpConf(1))
      .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + httpConf(0)))
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [http-server]"))
      .withFallback(defaultConf)
  ShareData.config = config
  
  implicit val system = ActorSystem("akkaflow", config)
  implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher
  val httpServer = system.actorOf(HttpServer.props,"http-server")
  
  
  val route =
    get {
      pathPrefix("item" / LongNumber){ id =>
        
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>Say hello to akka-http${id}</h1>"))
      } ~
      path("hello2") {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
        }
      }
  
   val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture.flatMap(_.unbind()) // trigger unbinding from the port
                 .onComplete(_ => system.terminate()) // and shutdown when done
}