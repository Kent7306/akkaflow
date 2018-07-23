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
import akka.actor.Actor
import akka.cluster.ClusterEvent._
import com.kent.pub.ClusterRole.Registration
import akka.actor.Props
import akka.actor.RootActorPath
import akka.util.Timeout
import scala.concurrent.duration._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.Http.ServerBinding
import com.kent.pub.ClusterRole
import com.kent.pub.Event._
import akka.actor.ActorRef
import akka.actor.Terminated
import scala.util.Success
/**
 * http接口服务角色
 */
class HttpServer extends ClusterRole {
  var activeMaster:ActorRef = _
  import scala.concurrent.ExecutionContext.Implicits.global
  private def getResponseFromWorkflowManager(sdr: ActorRef,event: Any) = {
    if(activeMaster != null){
    	val wfm = context.actorSelection(activeMaster.path / "wfm")
    			val resultF = (wfm ? event).mapTo[ResponseData]
    					resultF.andThen{
    					case Success(x) => sdr ! x
    	}
    }else{
      sdr ! ResponseData("fail","不存在workflow manager",null)
    }
  }
  private def getResponseFromMaster(sdr: ActorRef, event: Any) = {
    if(activeMaster != null){
      val master = context.actorSelection(activeMaster.path)
      val resultF = (master ? event).mapTo[ResponseData]
      resultF.andThen{
        case Success(x) => sdr ! x
      }
    }else{
      sdr ! ResponseData("fail","不存在活动的Master",null)
    }
  }
  
  def indivivalReceive: Actor.Receive = {
    case Terminated(ar) => 
      //若是活动节点，则删除
      if(ar == activeMaster){
        this.activeMaster = null
      }
    case SwitchActiveMaster() => 
      activeMaster = sender
      context.watch(activeMaster)
    case event@ShutdownCluster() => getResponseFromMaster(sender, event)
                              Thread.sleep(5000)
                              HttpServer.shutdwon()
    case event@CollectClusterActorInfo() => getResponseFromMaster(sender,event)
    case event@RemoveWorkFlow(_) =>  getResponseFromWorkflowManager(sender, event)
    case event@AddWorkFlow(_, _) => getResponseFromWorkflowManager(sender, event)
    case event@CheckWorkFlowXml(_) => getResponseFromWorkflowManager(sender, event)
    case event@ResetAllWorkflow() => getResponseFromWorkflowManager(sender, event)
    case event@ReRunWorkflowInstance(_,_) => getResponseFromWorkflowManager(sender, event)
    case event@KillWorkFlowInstance(_) => getResponseFromWorkflowManager(sender, event)
    case event@RemoveWorkFlowInstance(_) => getResponseFromWorkflowManager(sender, event)
    case event@Reset(_) => getResponseFromWorkflowManager(sender,event)
    case event@Trigger(_) => getResponseFromWorkflowManager(sender,event)
    case event@ManualNewAndExecuteWorkFlowInstance(_, _) => getResponseFromWorkflowManager(sender, event)
    case event@GetWaittingInstances() => getResponseFromWorkflowManager(sender, event)
    
  }
}
object HttpServer extends App{
  import org.json4s._
  import org.json4s.jackson.Serialization
  import org.json4s.jackson.Serialization.{read, write}
  implicit val formats = Serialization.formats(NoTypeHints)
  implicit val timeout = Timeout(20 seconds)
  def props = Props[HttpServer]
  val defaultConf = ConfigFactory.load()
  val httpConf = defaultConf.getStringList("workflow.nodes.http-servers").get(0).split(":")
  
  // 创建一个Config对象
  val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + httpConf(1))
      .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + httpConf(0)))
      .withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [${RoleType.HTTP_SERVER}]"))
      .withFallback(defaultConf)
  
  implicit val system = ActorSystem("akkaflow", config)
  implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher
  val httpServer = system.actorOf(HttpServer.props,RoleType.HTTP_SERVER)
  
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
          }else if(action == "run"){
             parameterMap { paras => 
                 handleRequestWithActor(ManualNewAndExecuteWorkFlowInstance(name, paras))
             }
          }else if(action == "reset"){
               handleRequestWithActor(Reset(name))                    
          }else if(action == "trigger"){
               handleRequestWithActor(Trigger(name))                    
          }else{
        	  handleRequestWithResponseData("fail","action参数有误",null)                      
          }
        }
      }
    } ~ path("akkaflow" / "workflow"){
      post {
        formField('xml){ content =>
          parameter('action){ action => {
            	if(action == "add" && content != null && content.trim() != ""){
            	  parameter('path){ path =>
            		  handleRequestWithActor(AddWorkFlow(content, path))              	    
            	  }
            	}else if(action == "check" && content != null && content.trim() != ""){
            	  handleRequestWithActor(CheckWorkFlowXml(content))  
            	}else{
            	 handleRequestWithResponseData("fail","action参数有误",null)    
            	}
            }
          }  
        }
      }
    } ~ path("akkaflow" / "workflows"){
      parameter('action){
          action => {
            if(action == "reset") handleRequestWithActor(ResetAllWorkflow())
            else handleRequestWithResponseData("fail","action参数有误",null)
          }
        }
    }
    //workflow instance
    val wfiRoute = path("akkaflow" / "workflow" / "instance" / Segment){            
      id => parameter('action){
        action => {
          if(action == "rerun"){
            handleRequestWithActor(ReRunWorkflowInstance(id, false))
          }else if(action == "kill"){
        	  handleRequestWithActor(KillWorkFlowInstance(id))                     
          }else if(action == "del"){
            handleRequestWithActor(RemoveWorkFlowInstance(id))
          }
          else{
        	  handleRequestWithActor("fail","action参数有误",null)                     
          }
        }
      }
    } ~ path("akkaflow" / "workflow" / "watting_instance" / "list"){
      handleRequestWithActor(GetWaittingInstances())
    }
    //cluster
    val clusterRoute = path("akkaflow" / "cluster"){            
      parameter('action){
        action => {
          if(action == "shutdown"){
            handleRequestWithActor(ShutdownCluster())
          }else if(action == "get"){
            handleRequestWithActor(CollectClusterActorInfo())
          }else{
        	  handleRequestWithActor("fail","action参数有误",null)                     
          }
        }
      }
    }
    
    val route = wfRoute ~ wfiRoute ~ clusterRoute
  
   val bindingFuture = Http().bindAndHandle(route, "localhost", 8090)
   def shutdwon(){
      bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate())
    }
}