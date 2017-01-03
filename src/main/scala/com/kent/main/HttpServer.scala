package com.kent.main

//import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.ask
import akka.pattern.pipe
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
import com.kent.workflow.WorkFlowManager._
import akka.actor.RootActorPath
import com.kent.main.HttpServer.ResponseData
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await

class HttpServer extends ClusterRole {
  implicit val timeout = Timeout(20 seconds)
  def getWorkflow(name: String): ResponseData = {
    ???
  }
  
  def addWorkflow(content: String): ResponseData = { 
    val wfm = context.actorSelection(roler(0).path / "wfm")
    val rdF = (wfm ? AddWorkFlow(content)).mapTo[ResponseData]
    val rd = Await.result(rdF, 20 second)
    rd
  }
  
  def removeWorkflow(name: String): ResponseData = {
    val wfm = context.actorSelection(roler(0).path / "wfm")
    val rdF = (wfm ? RemoveWorkFlow(name)).mapTo[ResponseData]
    val rd = Await.result(rdF, 20 second)
    rd
  }
  
  def receive: Actor.Receive = {
    case MemberUp(member) => 
    case UnreachableMember(member) =>
    case MemberRemoved(member, previousStatus) =>
    case state: CurrentClusterState =>
    case _:MemberEvent => // ignore 
    case Registration() => {
      //worker请求注册
      context watch sender
      roler = roler :+ sender
      log.info("Master registered: " + sender)
    }
    case RemoveWorkFlow(name) => sender ! removeWorkflow(name)
    case AddWorkFlow(content) => sender ! addWorkflow(content)
      
  }
}
object HttpServer extends App{
  import org.json4s._
  import org.json4s.jackson.Serialization
  import org.json4s.jackson.Serialization.{read, write}
  implicit val formats = Serialization.formats(NoTypeHints)
  implicit val timeout = Timeout(20 seconds)
  case class ResponseData(result:String, msg: String, data: String)
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
  
  
  val wfRoute =  path("akkaflow" / "workflow" / Segment){  //workflow
      name => parameter('action){
        action => {
          if(action == "del"){
            val data = (httpServer ? RemoveWorkFlow(name)).mapTo[ResponseData] 
        	  onSuccess(data){ x =>
        	    complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, write(x)))
            }
          }else if(action == "get"){
            ???
          }else{
        	  complete(HttpEntity(ContentTypes.`application/json`, write(ResponseData("fail","action参数有误",null))))                      
          }
        }
      }
    } ~ path("akkaflow" / "workflow"){
      parameter('action, 'content){
        (action, content) => {
        	if(action == "add" && content != null && content.trim() != ""){
        	  val data = (httpServer ? AddWorkFlow(content)).mapTo[ResponseData] 
        		onSuccess(data){ x =>
        	    complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, write(x)))
            }       	  
        	}else{
        	  complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, write(ResponseData("fail","action参数有误",null))))    
        	}
        }
      }
    }
    val coorRoute = path("akkaflow" / "coor" / Segment){            //coordinator
      id => parameter('action){
        action => {
          if(action == "del"){
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>coor delete ${id}</h1>"))                      
          }else if(action == "get"){
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>coor get ${id}</h1>"))                      
          }else{
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>error</h1>"))                      
          }
        }
      }
    } ~ path("akkaflow" / "coor"){
      parameter('action){
        action => {
        	complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>add coor</h1>"))          
        }
      }
    }
    
    val wfiRoute = path("akkaflow" / "workflow" / "instance" / Segment){            //workflow instance
      id => parameter('action){
        action => {
          if(action == "rerun"){
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>workflow instance rerun ${id}</h1>"))                      
          }else if(action == "kill"){
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>workflow instance kill ${id}</h1>"))                      
          }else{
        	  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>error</h1>"))                      
          }
        }
      }
    }
    
    val route = wfRoute ~ coorRoute ~ wfiRoute
  
   val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture.flatMap(_.unbind()) // trigger unbinding from the port
                 .onComplete(_ => system.terminate()) // and shutdown when done
}