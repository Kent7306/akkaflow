package com.kent.pub

import java.sql.ResultSet
import java.util.Date

import akka.actor.ActorRef
import com.kent.daemon.LogRecorder.LogType._
import com.kent.pub.db._
import com.kent.workflow.Coor.TriggerType._
import com.kent.workflow.Workflow.WStatus._
import com.kent.workflow.WorkflowInstance
import com.kent.workflow.node.Node.Status._
import com.kent.workflow.node.action.ActionNodeInstance

object Event {
  //pub
  case class Tick()
  //master
  case class GetWorker(workerOpt: Option[ActorRef])
  case class AskWorker(host: String)
  case class ShutdownCluster()
  case class NotifyActive(masterRef: ActorRef)
  //cm
  case class StartIfActive(isActve: Boolean)
  case class Start()
  case class Stop()
  case class AddCoor(content: String)
  case class CheckCoorXml(xmlStr: String)
  case class RemoveCoor(name: String)
  case class UpdateCoor(content: String)
  
  //log-recorder
  case class Info(stime: Date,ctype: LogType, sid: String,name: String, content: String)
  case class Warn(stime: Date,ctype: LogType, sid: String,name: String, content: String)
  case class Error(stime: Date,ctype: LogType, sid: String,name: String, content: String)
  case class GetLog(ctype: LogType, sid: String,name: String)
  //xml-loader
  case class AddDBLink(dbl: DBLink)
  case class GetDBLink(name: String)
  case class DBLinkRequst(action: String, name: String, dbtype: String, jdbcUrl: String, username: String, password: String, desc: String)
  
  //persist-manager
  //case class Save[A](obj: Persistable[A])
  //case class Delete[A](obj: Persistable[A])
  //case class Get[A](obj: Persistable[A])
  //case class Query(query: String)
  //case class ExecuteSql(sql: String)

  case class ExecuteSqls(sqls: List[String])
  case class QuerySql[A](sql: String, rsHandler:ResultSet => A)

  //email-sender
  case class EmailMessage(toUsers: List[String],subject: String,htmlText: String, attachFiles: List[String])
  //wfm
  case class Trigger(name: String, triggerType: TriggerType)
  case class Reset(wfName: String)
  case class ResetAllWorkflow()
  case class ManualNewAndExecuteWorkFlowInstance(wfName: String, params: Map[String, String])
  case class KillWorkFlow(wfName: String)
  case class KllAllWorkFlow()
  case class KillWorkFlowInstance(id: String)
  case class AddWorkFlow(xmlStr: String, filePath: String)
  case class CheckWorkFlowXml(xmlStr: String)
  case class RemoveWorkFlow(wfName: String)
  case class RemoveWorkFlowInstance(id: String)
  case class ReRunWorkflowInstance(worflowInstanceId: String, isFormer: Boolean)
  case class WorkFlowInstanceExecuteResult(workflowInstance: WorkflowInstance)
  case class WorkFlowExecuteResult(wfName: String, status: WStatus)
  //读取文件内容
  case class FileContent(isSuccessed: Boolean, msg: String,path: String, content:Array[Byte])
  case class GetFileContent(path: String)
  //wf-actor
  case class Kill()
  case class MailMessage(msg: String)
  
  //worker
  case class CreateAction(ani: ActionNodeInstance)
  case class RemoveAction(name: String)
  case class KillAllActionActor()
  //action actor
  case class ActionExecuteResult(status: Status, msg: String) extends Serializable
  case class Termination()
  case class GetInstanceShortInfo()
  case class InstanceShortInfo(id: String, name: String, desc: String, dir: String, owner: String, dependWfNames: List[String])
  
  //lineage-record
  //case class SaveLineageRecord(lr: LineageRecord)
  case class DelLineageTable(tableName: String)
  
  //http-server
  case class ResponseData(result:String, msg: String, data: Any)
  //case class SwitchActiveMaster()
  //收集集群信息
  case class CollectClusterActorInfo()
  case class CollectActorInfo()
  //今天剩余触发的次数
  case class GetTodayLeftTriggerCnt(wfName: String)
  case class GetTodayAllLeftTriggerCnt()


  //后面ask的返回最好都采用这种好一点
  case class Result(isSuccess: Boolean, msg: String, dataOpt: Option[Any]) extends Serializable{
    def isFail = !isSuccess
    def isHasData: Boolean = dataOpt.isDefined
    def toDataOpt[A]: Option[A] = if (dataOpt.isDefined) Some(dataOpt.get.asInstanceOf[A]) else None
    def isSuccessAndHasData = if(isSuccess && dataOpt.isDefined) true else false
    def data[A]: A = if (dataOpt.isDefined) dataOpt.get.asInstanceOf[A] else throw new Exception("data值不存在")
  }
}