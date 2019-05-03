package com.kent.main

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.cluster.Member
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern._
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.kent.pub.Event._
import com.kent.pub.actor.ClusterRole
import com.kent.workflow.Coor.TriggerType
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * http接口服务角色
  */
class HttpServer extends ClusterRole {
  var activeMasterOpt: Option[ActorRef] = None

  import scala.concurrent.ExecutionContext.Implicits.global

  /**
    * 处理请求事件
    * @param event
    * @param daemonName
    * @return
    */
  private def getResponseFromMaster(event: Any, daemonName: String): Future[ResponseData] = {
    if (activeMasterOpt.isDefined) {
      val wfm = if(daemonName == null) {
        context.actorSelection(activeMasterOpt.get.path)
      } else {
        context.actorSelection(activeMasterOpt.get.path / daemonName)
      }
      val resultF = (wfm ? event).mapTo[ResponseData]
      resultF.recover{
        case e: Exception => ResponseData("fail", "不存在活动的Master", null)
      }
    } else {
      Future {ResponseData("fail", "不存在活动的Master", null)}
    }
  }

  override def onRoleMemberUp(member: Member): Unit = {}

  /**
    * 有其他角色退出集群
    *
    * @param member
    */
  override def onRoleMemberRemove(member: Member): Unit = {
    //活动master退出
    if (member.hasRole(ClusterRole.MASTER) && activeMasterOpt.isDefined){
      if (member.address == activeMasterOpt.get.path.address){
        this.activeMasterOpt = None
      }
    }

  }

  def individualReceive: Actor.Receive = {
    case event@ShutdownCluster() =>
      getResponseFromMaster(event, null).onComplete{
        case _ => HttpServer.shutdown()
      }
    case event:CollectClusterActorInfo => getResponseFromMaster(event, null) pipeTo sender
    case event:RemoveWorkFlow => getResponseFromMaster(event, "wfm") pipeTo sender
    case event:AddWorkFlow => getResponseFromMaster(event, "wfm") pipeTo sender
    case event:CheckWorkFlowXml => getResponseFromMaster(event,"wfm") pipeTo sender
    case event:ResetAllWorkflow => getResponseFromMaster(event,"wfm") pipeTo sender
    case event:GetTodayAllLeftTriggerCnt => getResponseFromMaster(event,"wfm") pipeTo sender
    case event:ReRunWorkflowInstance => getResponseFromMaster(event, "wfm") pipeTo sender
    case event:KillWorkFlowInstance => getResponseFromMaster(event, "wfm") pipeTo sender
    case event:RemoveWorkFlowInstance => getResponseFromMaster(event, "wfm") pipeTo sender
    case event:Reset => getResponseFromMaster(event, "wfm") pipeTo sender
    case event:Trigger => getResponseFromMaster(event, "wfm") pipeTo sender
    case event:ManualNewAndExecuteWorkFlowInstance => getResponseFromMaster(event,"wfm") pipeTo sender
    case event:DBLinkRequst => getResponseFromMaster(event,"xml-loader") pipeTo sender


  }

  /**
    * 活动master通知
    *
    * @param masterRef
    * @return
    */
  override def notifyActive(masterRef: ActorRef): Result = {
    log.info(s"设置master引用: ${masterRef.path}")
    this.activeMasterOpt = Some(masterRef)
    Result(true, "", None)
  }
}

object HttpServer extends App {

  import org.json4s._
  import org.json4s.jackson.Serialization
  import org.json4s.jackson.Serialization.write

  implicit val formats = Serialization.formats(NoTypeHints)
  implicit val timeout = Timeout(20 seconds)

  def props = Props[HttpServer]

  val defaultConf = ConfigFactory.load()
  val serverPort = defaultConf.getInt("workflow.node.http-server.port")
  val httpPort = defaultConf.getInt("workflow.node.http-server.connector-port")
  val hostIp = defaultConf.getString("workflow.current.inner.hostname")
  // 创建一个Config对象
  val config = ConfigFactory.parseString("akka.remote.artery.canonical.port=" + serverPort)
    .withFallback(ConfigFactory.parseString("akka.remote.artery.bind.port=" + serverPort))
    .withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [${ClusterRole.HTTP_SERVER}]"))
    .withFallback(defaultConf)

  implicit val system = ActorSystem("akkaflow", config)
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher
  val httpServer = system.actorOf(HttpServer.props, ClusterRole.HTTP_SERVER)

  private def handleRequestWithActor(event: Any): Route = {
    val data = (httpServer ? event).mapTo[ResponseData]
    onSuccess(data) { x =>
      complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, write(x)))
    }
  }

  private def handleRequestWithResponseData(result: String, msg: String, data: String): Route = {
    complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, write(ResponseData(result, msg, data))))
  }

  //workflow
  val wfRoute = path("akkaflow" / "workflow" / Segment) {
    name =>
      parameter('action) {
        action => {
          if (action == "del") {
            handleRequestWithActor(RemoveWorkFlow(name))
          } else if (action == "get") {
            handleRequestWithResponseData("fail", "暂时还没get方法", null)
          } else if (action == "run") {
            parameterMap { paras =>
              handleRequestWithActor(ManualNewAndExecuteWorkFlowInstance(name, paras))
            }
          } else if (action == "reset") {
            handleRequestWithActor(Reset(name))
          } else if (action == "trigger") {
            handleRequestWithActor(Trigger(name, TriggerType.NEXT_SET))
          } else if (action == "trigger-blood") {
            handleRequestWithActor(Trigger(name, TriggerType.BLOOD_EXCUTE))
          } else {
            handleRequestWithResponseData("fail", "action参数有误", null)
          }
        }
      }
  } ~ path("akkaflow" / "workflow") {
    post {
      formField('xml) { content =>
        parameter('action) { action => {
          if (action == "add" && content != null && content.trim() != "") {
            parameter('path) { path =>
              handleRequestWithActor(AddWorkFlow(content, path))
            }
          } else if (action == "check" && content != null && content.trim() != "") {
            handleRequestWithActor(CheckWorkFlowXml(content))
          } else {
            handleRequestWithResponseData("fail", "action参数有误", null)
          }
        }
        }
      }
    }
  } ~ path("akkaflow" / "workflows") {
    parameter('action) {
      action => {
        if (action == "reset") handleRequestWithActor(ResetAllWorkflow())
        else if (action == "today-trigger-left") handleRequestWithActor(GetTodayAllLeftTriggerCnt())
        else handleRequestWithResponseData("fail", "action参数有误", null)
      }
    }
  }

  val dbLinkRoute = path("akkaflow" / "db-link") {
    post {
      parameter('action) { action => {
          if (action == "add") {
            formField('name, 'dbtype, 'jdbcUrl, 'username, 'password, 'desc){
              (name, dbtype, jdbcUrl, username, password, desc) =>
                handleRequestWithActor(DBLinkRequst(action, name, dbtype, jdbcUrl, username, password, desc))
            }
          }
          else handleRequestWithResponseData("fail", "action参数有误", null)
        }
      }
    }
  } ~ path("akkaflow" / "db-link" / Segment) { name =>
    parameter('action) {
      action => {
        if (action == "update") {
          formField('dbtype, 'jdbcUrl, 'username, 'password, 'desc){
            ( dbtype, jdbcUrl, username, password, desc) =>
              handleRequestWithActor(DBLinkRequst(action, name, dbtype, jdbcUrl, username, password, desc))
          }
        }
        else if(action == "del") {
          handleRequestWithActor(DBLinkRequst(action, name, null, null, null, null, null))
        }
        else handleRequestWithResponseData("fail", "action参数有误", null)
      }
    }
  }




  //workflow instance
  val wfiRoute = path("akkaflow" / "workflow" / "instance" / Segment) {
    id =>
      parameter('action) {
        action => {
          if (action == "rerun") {
            handleRequestWithActor(ReRunWorkflowInstance(id, false))
          } else if (action == "kill") {
            handleRequestWithActor(KillWorkFlowInstance(id))
          } else if (action == "del") {
            handleRequestWithActor(RemoveWorkFlowInstance(id))
          }
          else {
            handleRequestWithActor("fail", "action参数有误", null)
          }
        }
      }
  }
  //cluster
  val clusterRoute = path("akkaflow" / "cluster") {
    parameter('action) {
      action => {
        if (action == "shutdown") {
          handleRequestWithActor(ShutdownCluster())
        } else if (action == "get") {
          handleRequestWithActor(CollectClusterActorInfo())
        } else {
          handleRequestWithActor("fail", "action参数有误", null)
        }
      }
    }
  }

  val route = wfRoute ~ wfiRoute ~ clusterRoute ~ dbLinkRoute

  val bindingFuture = Http().bindAndHandle(route, hostIp, httpPort)

  def shutdown() {
    bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate())
  }
}