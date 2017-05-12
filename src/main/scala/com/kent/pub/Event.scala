package com.kent.pub

import akka.actor.ActorRef
import com.kent.workflow.node.ActionNodeInstance
import com.kent.workflow.WorkflowInstance
import com.kent.workflow.WorkflowInfo.WStatus._
import com.kent.workflow.node.NodeInfo.Status._

object Event {
  //master
  case class GetWorker(worker: ActorRef)
  case class AskWorker(host: String)
  case class ShutdownCluster()
  //cm
  case class Start()
  case class Stop()
  case class AddCoor(content: String)
  case class RemoveCoor(name: String)
  case class UpdateCoor(content: String)
  case class GetManagers(workflowManager: ActorRef, coorManager: ActorRef)
  //log-recorder
  case class Info(ctype: String, sid: String, content: String)
  case class Warn(ctype: String, sid: String, content: String)
  case class Error(ctype: String, sid: String, content: String)
  //persist-manager
  case class Save[A](obj: Daoable[A])
  case class Delete[A](obj: Daoable[A])
  case class Get[A](obj: Daoable[A])
  case class Query(query: String)
  //email-sender
  case class EmailMessage(toUsers: List[String],subject: String,htmlText: String)
  //wfm
  case class NewAndExecuteWorkFlowInstance(wfName: String, params: Map[String, String])
  case class ManualNewAndExecuteWorkFlowInstance(wfName: String, params: Map[String, String])
  case class KillWorkFlow(wfName: String)
  case class KllAllWorkFlow()
  case class KillWorkFlowInstance(id: String)
  case class AddWorkFlow(content: String)
  case class RemoveWorkFlow(wfName: String)
  case class ReRunWorkflowInstance(worflowInstanceId: String)
  case class WorkFlowInstanceExecuteResult(workflowInstance: WorkflowInstance)
  case class WorkFlowExecuteResult(wfName: String, status: WStatus)
  //wf-actor
  case class Kill()
  case class MailMessage(msg: String)
  
  //worker
  case class CreateAction(ani: ActionNodeInstance)
  //action actor
  case class ActionExecuteResult(status: Status, msg: String) extends Serializable
  case class ActionExecuteRetryTimes(times: Int) extends Serializable
  
  //http-server
  case class ResponseData(result:String, msg: String, data: Any)
  
  //收集集群信息
  case class CollectClusterInfo()
  case class GetClusterInfo(ai: ActorInfo)
  
  class ActorInfo extends Serializable{
	  import com.kent.pub.Event.ActorType._
    var name: String = _
    var ip: String = _
    var port: Int = _
    var atype: ActorType = ACTOR
    var subActors:List[ActorInfo] = List()
    
    def getClusterInfo():List[Map[String, String]] = {
	    var l:List[Map[String, String]] = List()
	    val l1 = this.subActors.map { x => 
	      val m = Map("name" -> x.name, "ip" -> x.ip, "port" -> s"${x.port}", "atype" -> s"${x.atype.id}", "pname" -> name)
	      m
	    }.toList  
	    val l2 = this.subActors.flatMap { x => x.getClusterInfo() }.toList
	    l = l ++ l1
	    l = l ++ l2
	    l
	  }
    
  }
  object ActorType extends Enumeration {
	  type ActorType = Value
			  val ROLE,DEAMO,ACTOR = Value
  }
  
  
}