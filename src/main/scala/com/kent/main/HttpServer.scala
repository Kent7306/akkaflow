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
import com.kent.pub._
import com.kent.pub.actor.{ClusterRole, Daemon}
import com.kent.pub.db.DBLink
import com.kent.pub.db.DBLink.DatabaseType
import com.kent.pub.io.FileLink
import com.kent.pub.io.FileLink.FileSystemType
import com.kent.util.Util
import com.kent.workflow.Coor.TriggerType
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

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
  private def getResponseFromMaster(event: Any, daemonName: String): Future[Result] = {
    if (activeMasterOpt.isDefined) {
      val wfm = if(daemonName == null) {
        context.actorSelection(activeMasterOpt.get.path)
      } else {
        context.actorSelection(activeMasterOpt.get.path / daemonName)
      }
      val resultF = (wfm ? event).mapTo[Result]
      resultF.recover{
        case e: Exception => FailedResult("不存在活动的Master")
      }
    } else {
      Future {FailedResult("不存在活动的Master")}
    }
  }

  private def getResponseFromMaster(event: Any): Future[Result] = {
    getResponseFromMaster(event, null)
  }

  override def onOtherMemberUp(member: Member): Unit = {}
  override def onSelfMemberUp(): Unit = {}

  /**
    * 有其他角色退出集群
    *
    * @param member
    */
  override def onMemberRemove(member: Member): Unit = {
    //活动master退出
    if (member.hasRole(ClusterRole.MASTER) && activeMasterOpt.isDefined){
      if (member.address == activeMasterOpt.get.path.address){
        this.activeMasterOpt = None
      }
    }

  }

  def individualReceive: Actor.Receive = {
    case event@ShutdownCluster() => getResponseFromMaster(event) pipeTo sender
    case event:CollectClusterActorInfo => getResponseFromMaster(event, null) pipeTo sender
    case event:RemoveWorkFlow => getResponseFromMaster(event, Daemon.WORKFLOW_MANAGER) pipeTo sender
    case event:AddWorkFlow => getResponseFromMaster(event, Daemon.WORKFLOW_MANAGER) pipeTo sender
    case event:CheckWorkFlowXml => getResponseFromMaster(event,Daemon.WORKFLOW_MANAGER) pipeTo sender
    case event:ResetAllWorkflow => getResponseFromMaster(event,Daemon.WORKFLOW_MANAGER) pipeTo sender
    case event:GetTodayAllLeftTriggerCnt => getResponseFromMaster(event,Daemon.WORKFLOW_MANAGER) pipeTo sender
    case event:ReRunWorkflowInstance => getResponseFromMaster(event, Daemon.WORKFLOW_MANAGER) pipeTo sender
    case event:KillWorkFlowInstance => getResponseFromMaster(event, Daemon.WORKFLOW_MANAGER) pipeTo sender
    case event:RemoveWorkFlowInstance => getResponseFromMaster(event, Daemon.WORKFLOW_MANAGER) pipeTo sender
    case event:Reset => getResponseFromMaster(event, Daemon.WORKFLOW_MANAGER) pipeTo sender
    case event:Trigger => getResponseFromMaster(event, Daemon.WORKFLOW_MANAGER) pipeTo sender
    case event:ManualNewAndExecuteWorkFlowInstance => getResponseFromMaster(event,Daemon.WORKFLOW_MANAGER) pipeTo sender
    case event:DBLinkRequest => getResponseFromMaster(event,Daemon.XML_LOADER) pipeTo sender
    case event:FileLinkRequest => getResponseFromMaster(event,Daemon.XML_LOADER) pipeTo sender
    case event:DelWorklowDir => getResponseFromMaster(event, Daemon.WORKFLOW_MANAGER) pipeTo sender
  }

  /**
    * 活动master通知
    *
    * @param masterRef
    * @return
    */
  override def notifyActive(masterRef: ActorRef): Future[Result] = {
    log.info(s"设置master引用: ${masterRef.path}")
    this.activeMasterOpt = Some(masterRef)
    Future{SucceedResult()}
  }
}

object HttpServer extends App {

  implicit val timeout = Timeout(20 seconds)

  def props = Props[HttpServer]

  val defaultConf = ConfigFactory.load()
  val serverPort = defaultConf.getInt("workflow.node.http-server.port")
  val httpPort = defaultConf.getInt("workflow.node.http-server.http-port")
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
    val data = (httpServer ? event).mapTo[Result]
    onSuccess(data) { x =>
      val route = complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, x.toJsonString))
      if (event.isInstanceOf[ShutdownCluster]) {
        bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate())
      }
      route
    }
  }

  private def handleRequestWithResponseData(isSucceed: Boolean, msg: String, dataOpt: Option[Any]): Route = {
    val result = if (isSucceed) SucceedResult(msg, dataOpt) else FailedResult(msg, dataOpt)
    complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, result.toJsonString))
  }

  //workflow
  val wfRoute = path("akkaflow" / "workflow" / Segment) {
    name =>
      parameter('action) {
        action => {
          if (action == "del") {
            handleRequestWithActor(RemoveWorkFlow(name))
          } else if (action == "get") {
            handleRequestWithResponseData(false, "暂时还没get方法", None)
          } else if (action == "run") {
            parameterMap { paras =>
              handleRequestWithActor(ManualNewAndExecuteWorkFlowInstance(name, paras))
            }
          } else if (action == "reset") {
            handleRequestWithActor(Reset(name))
          } else if (action == "trigger") {
            parameter('ignore_node) {
              ignore_node => {
                if (ignore_node == null || ignore_node.trim == ""){
                  handleRequestWithActor(Trigger(name, None, TriggerType.NEXT_SET))
                } else {
                  val nodeNames = ignore_node.split(",").toList
                  handleRequestWithActor(Trigger(name, Some(nodeNames), TriggerType.NEXT_SET))
                }

              }
            }
          } else if (action == "trigger-blood") {
            handleRequestWithActor(Trigger(name, None, TriggerType.BLOOD_EXCUTE))
          } else {
            handleRequestWithResponseData(false, "action参数有误", None)
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
          }
          else {
            handleRequestWithResponseData(false, "action参数有误", None)
          }
        }
        }
      }
    } ~ get {
      parameter('action) { action => {
        if (action == "del-dir") {
          parameter('id) { id =>
            handleRequestWithActor(DelWorklowDir(id.toInt))
          }
        } else {
          handleRequestWithResponseData(false, "action参数有误", None)
        }
      }
      }
    }
  } ~ path("akkaflow" / "workflows") {
    parameter('action) {
      action => {
        if (action == "reset") handleRequestWithActor(ResetAllWorkflow())
        else if (action == "today-trigger-left") handleRequestWithActor(GetTodayAllLeftTriggerCnt())
        else handleRequestWithResponseData(false, "action参数有误", None)
      }
    }
  }

  val dbLinkRoute = path("akkaflow" / "db-link") {
    post {
      parameter('action) { action => {
          if (action == "add") {
            formField('name, 'dbtype, 'jdbcUrl, 'username, 'password, 'desc, 'properties){
              (name, dbtypeStr, jdbcUrl, username, password, desc, properties) =>
                Try {
                  val dbtype = DatabaseType.withName(dbtypeStr)
                  val dbl = DBLink(dbtype, name, jdbcUrl, username, password, desc, properties)
                  handleRequestWithActor(DBLinkRequest(Action.ADD, dbl))
                }.recover{
                  case e: Exception => handleRequestWithResponseData(false, e.getMessage, None)
                }.get
            }
          }
          else handleRequestWithResponseData(false, "action参数有误", None)
        }
      }
    }
  } ~ path("akkaflow" / "db-link" / Segment) { name =>
    parameter('action) {
      action => {
        if (action == "update") {
          formField('dbtype, 'jdbcUrl, 'username, 'password, 'desc, 'properties){
            ( dbtypeStr, jdbcUrl, username, password, desc, properties) =>
              Try {
                val dbtype = DatabaseType.withName(dbtypeStr)
                val dbl = DBLink(dbtype, name, jdbcUrl, username, password, desc, properties)
                handleRequestWithActor(DBLinkRequest(Action.UPDATE, dbl))
              }.recover{
                case e: Exception => handleRequestWithResponseData(false, e.getMessage, None)
              }.get
          }
        }
        else if(action == "del") {
          val dbl = DBLink(name)
          handleRequestWithActor(DBLinkRequest(Action.DEL, dbl))
        }
        else handleRequestWithResponseData(false, "action参数有误", None)
      }
    }
  }

  val fileLinkRoute = path("akkaflow" / "file-link") {
    post {
      parameter('action) { action => {
        if (action == "add") {
          formField('name, 'fstype, 'host, 'port, 'username, 'password, 'desc){
            (name, fstype, host, port, username, password, desc) =>
              val fs = FileSystemType.withName(fstype)
              if (Util.isNumber(port)){
                val fl = FileLink(fs, name, host, port.toInt, username, password, desc)
                handleRequestWithActor(FileLinkRequest(Action.ADD, fl))
              } else {
                handleRequestWithResponseData(false, "端口port不为整数", None)
              }
          }
        }
        else handleRequestWithResponseData(false, "action参数有误", None)
      }
      }
    }
  } ~ path("akkaflow" / "file-link" / Segment) { name =>
    parameter('action) {
      action => {
        if (action == "update") {
          formField('fstype, 'host, 'port, 'username, 'password, 'desc){
            (fstype, host, port, username, password, desc) =>
              val fs = FileSystemType.withName(fstype)
              val fl = FileLink(fs, name, host, port.toInt, username, password, desc)
              handleRequestWithActor(FileLinkRequest(Action.UPDATE, fl))
          }
        }
        else if(action == "del") {
          val fl = FileLink(name)
          handleRequestWithActor(FileLinkRequest(Action.DEL, fl))
        }
        else handleRequestWithResponseData(false, "action参数有误", None)
      }
    }
  }




  //workflow instance
  val wfiRoute = path("akkaflow" / "workflow" / "instance" / Segment) {
    id =>
      parameter('action) { action => {
          if (action == "rerun") {
            parameter('recover) {
              case "true" =>
              handleRequestWithActor(ReRunWorkflowInstance(id, false, true))
              case _ =>
                handleRequestWithActor(ReRunWorkflowInstance(id, false, false))
            }
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

  val route = wfRoute ~ wfiRoute ~ clusterRoute ~ dbLinkRoute ~ fileLinkRoute

  val bindingFuture = Http().bindAndHandle(route, hostIp, httpPort)

  object Action extends Enumeration {
    type Action = Value
    val ADD, DEL, UPDATE, MERGE, GET,   //基本操作
       TRIGGER, KILL, RERUN = Value
  }
}