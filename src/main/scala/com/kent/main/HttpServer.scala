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
import com.kent.coordinate.CoordinatorManager._
import akka.http.scaladsl.server.Route

class HttpServer extends ClusterRole {
  implicit val timeout = Timeout(20 seconds)
  
  private def getResponseFromWorkflowManager(event: Any): ResponseData = {
    val wfm = context.actorSelection(roler(0).path / "wfm")
    val resultF = (wfm ? event).mapTo[ResponseData]
    val result = Await.result(resultF, 20 second)
    result
  }
  private def getResponseFromCoordinatorManager(event: Any): ResponseData = {
    val cm = context.actorSelection(roler(0).path / "cm")
    val resultF = (cm ? event).mapTo[ResponseData]
    val result = Await.result(resultF, 20 second)
    result
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
    case RemoveWorkFlow(name) => sender ! getResponseFromWorkflowManager(RemoveWorkFlow(name))
    case AddWorkFlow(content) => sender ! getResponseFromWorkflowManager(AddWorkFlow(content))
    case ReRunWorkflowInstance(id) => sender ! getResponseFromWorkflowManager(ReRunWorkflowInstance(id))
    case KillWorkFlowInstance(id) => sender ! getResponseFromWorkflowManager(KillWorkFlowInstance(id))
    case AddCoor(content) => sender ! getResponseFromCoordinatorManager(AddCoor(content))
    case RemoveCoor(name) => sender ! getResponseFromCoordinatorManager(RemoveCoor(name))
      
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
  
  private def handleRequestWithActor(event: Any): Route = {
    val data = (httpServer ? event).mapTo[ResponseData] 
	  onSuccess(data){ x =>
	    complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, write(x)))
    }
  }
  private def handleRequestWithResponseData(result: String, msg: String, data: String): Route = {
     complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, write(ResponseData(result,msg,data))))   
  }
  
   //workflow
  val wfRoute =  path("akkaflow" / "workflow" / Segment){ 
      name => 
        parameter('action){
        action => {
          if(action == "del"){
            handleRequestWithActor(RemoveWorkFlow(name))
          }else if(action == "get"){
            handleRequestWithResponseData("fail","暂时还没get方法",null)   
          }else{
        	  handleRequestWithResponseData("fail","action参数有误",null)                      
          }
        }
      }
    } ~ path("akkaflow" / "workflow"){
      post {
        formField('content){ content =>
          parameter('action){ action => {
            	if(action == "add" && content != null && content.trim() != ""){
            	  handleRequestWithActor(AddWorkFlow(content))  
            	}else{
            	 handleRequestWithResponseData("fail","action参数有误",null)    
            	}
            }
          }
        }
      }
    }
    //coordinator
    val coorRoute = path("akkaflow" / "coor" / Segment){           
      name => parameter('action){
        action => {
          if(action == "del"){
           handleRequestWithActor(RemoveCoor(name))                    
          }else if(action == "get"){
        	  handleRequestWithResponseData("fail","暂时还没get方法",null)                      
          }else{
        	  handleRequestWithResponseData("fail","action参数有误",null)                     
          }
        }
      }
    } ~ path("akkaflow" / "coor"){
      post{
        formField('content){ content =>
          parameter('action){
            action => {
              if(action == "add" && content != null && content.trim() != ""){
            	  handleRequestWithActor(AddCoor(content)) 	  
            	}else{
            	  handleRequestWithResponseData("fail","action参数有误",null)    
            	}
            }
          }
        }
      }
    }
    //workflow instance
    val wfiRoute = path("akkaflow" / "workflow" / "instance" / Segment){            
      id => parameter('action){
        action => {
          if(action == "rerun"){
            handleRequestWithActor(ReRunWorkflowInstance(id))
          }else if(action == "kill"){
        	  handleRequestWithActor(KillWorkFlowInstance(id))                     
          }else{
        	  handleRequestWithActor("fail","action参数有误",null)                     
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