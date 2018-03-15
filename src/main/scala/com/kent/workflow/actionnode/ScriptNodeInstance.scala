package com.kent.workflow.actionnode

import com.kent.workflow.node.ActionNodeInstance
import com.kent.workflow.node.NodeInstance
import scala.sys.process.ProcessLogger
import com.kent.main.Worker
import com.kent.pub.Event._
import com.kent.coordinate.ParamHandler
import scala.sys.process._
import java.util.Date
import java.io.PrintWriter
import java.io.File
import com.kent.util.Util
import com.kent.db.LogRecorder.LogType
import com.kent.db.LogRecorder.LogType._
import com.kent.db.LogRecorder
import akka.pattern.{ ask, pipe }
import scala.concurrent.ExecutionContext.Implicits.global
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.Await
import com.kent.util.FileUtil

class ScriptNodeInstance(override val nodeInfo: ScriptNode) extends ActionNodeInstance(nodeInfo)  {
  private var executeProcess: Process = _
  implicit val timeout = Timeout(60 seconds)
  
  override def execute(): Boolean = {
    val resultF = this.writeAttachFiles(this.nodeInfo.attachFiles)
    val result = Await.result(resultF, 120 seconds)
    if(result){
    	val paramLineOpt = if(nodeInfo.paramLine == null) None else Some(nodeInfo.paramLine)
    	this.executeScript(nodeInfo.code, paramLineOpt){
    		process => this.executeProcess = process
    	}      
    }else{
      false
    }
  }

  def kill(): Boolean = {
    if(executeProcess != null){
      executeProcess.destroy()
    }
    true
  }
}